from typing import Any
import psycopg2
from psycopg2.extras import execute_values
from psycopg2._psycopg import connection as Connection
import logging as log


# log = logging.getLogger(__name__)


def connect(uri: str):
    """Connects to pg using uri like 'postgresql://user:pw@host:port'.

    Args:
        uri (str): Address with credentials and port for database.

    Returns:
        psycopg2._psycopg.connection: Handles the connection to a
            PostgreSQL database instance.

    Example:
        >>> from trz_py_utils.db import connect
        >>> import os
        >>> print(os.environ.get("PG_DB_URI"))
        postgresql://root:password@postgres:5432/trz
        >>> connect(os.environ.get("PG_DB_URI"))
        <connection object at ...
    """
    log.info("connecting to database...")
    try:
        conn = psycopg2.connect(uri)
        log.info("connected!")
        return conn
    except Exception as e:
        log.error(f"unable to connect to database: {e}")
        raise


def fast_insert_into(connection: Connection,
                     table: str,
                     rows: list[dict[str, Any]],
                     sql: str = None,
                     **kwargs):
    """sends entire SQL string with values from cursor.mogrify()
    https://stackoverflow.com/a/10147451/5563327

    Example:
        >>> from trz_py_utils import db
        >>> import os; uri = os.environ.get("PG_DB_URI");
        >>> db.execute_sql(connect(uri), "CREATE TABLE fast(col1 VARCHAR);")
        >>> db.fast_insert_into(
        ...     connection=connect(uri),
        ...     table="fast",
        ...     rows=[{"col1": 1}, {"col1": 2}, {"col1": 3}, {"col1": 4}], )
        >>> db.execute_sql(connect(uri), "SELECT * FROM fast", is_return=True)
        [('1',), ('2',), ('3',), ('4',)]

    Example:
        >>> from trz_py_utils import db
        >>> import os; uri = os.environ.get("PG_DB_URI");
        >>> db.execute_sql(connect(uri), "CREATE TABLE fast2(col1 VARCHAR);")
        >>> db.fast_insert_into(
        ...     connection=connect(uri),
        ...     table="fast",
        ...     rows=[], )
        >>> db.execute_sql(connect(uri), "SELECT * FROM fast2", is_return=True)
        []
    """
    if not isinstance(rows, list):
        raise ValueError("must pass list[dict[str, Any]]!")
    elif not len(rows):
        log.info("skipping 0-length rows")
        return
    elif not isinstance(rows[0], dict):
        raise ValueError("must pass list[dict[str, Any]]!")
    num_rows = len(rows)

    cols = ",".join(rows[0].keys())
    row_tuples = tuple(tuple(row.values()) for row in rows)
    sql = sql or f"INSERT INTO {table} ({cols}) VALUES %s"
    log.debug(f"cols={cols}, rows={row_tuples}")
    try:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, row_tuples, **kwargs)
        connection.commit()
        log.info(f"inserted {num_rows} rows.")
    except Exception as e:
        if connection:
            connection.rollback()
        raise e


def make_sql_insert_into(table: str,
                         data: dict[str, Any]):
    """
    Makes a SQL string to create a row of data from a table name
    and dictionary of whose keys are column names.

    Example:

    >>> from trz_py_utils.db import make_sql_insert_into
    >>> data = {"mycol1": "myval1", "mycol2": 2}
    >>> make_sql_insert_into("mytable", data)
    'INSERT INTO mytable (mycol1, mycol2) VALUES (%s, %s)'
    """
    log.info(f"making 'INSERT' SQL for '{table}'...")
    columns = ', '.join(data.keys())
    vals = ["%s" for v in data.values()]
    sql = f"INSERT INTO {table} ({columns}) VALUES ({', '.join(vals)})"
    log.info("done making INSERT INTO statement.")

    return sql


def execute_sql(connection: Connection, sql: str,
                values: tuple = None, is_return=False):
    """Tries its best to run SQL statements and recover from errors.

    Args:
        connection (Connection): psycopg connection (`connect(url)`).
        sql (str): SQL statement to execute on the database.
        values (tuple, optional): variables to interpolate into SQL statement.
            Defaults to None.
        is_return (bool, optional): whether or not run `cursor.fetchall()`
            and return its response. Defaults to False.

    Raises:
        Exception: any exception from psycopg, after `connection.rollback()`

    Returns:
        list[tuple[Any]] | None: if `is_return` is truthy, the db response.

    Example:
        >>> from trz_py_utils.db import connect, execute_sql
        >>> import os; uri = os.environ.get("PG_DB_URI");
        >>> sql_statement = '''
        ...     CREATE TABLE IF NOT EXISTS t2(col1 VARCHAR);
        ...     INSERT INTO t2 (col1) VALUES (%s);
        ...     '''
        >>> execute_sql(
        ...     connection=connect(uri),
        ...     sql=sql_statement,
        ...     values=[("val1", ), ("val2", )], )

    Example:
        >>> from trz_py_utils.db import connect, execute_sql
        >>> import os; uri = os.environ.get("PG_DB_URI");
        >>> print(uri)
        postgresql://root:password@postgres:5432/trz
        >>> connection = connect(uri)
        >>> sql_statement = '''
        ...     SELECT *
        ...     FROM information_schema.tables
        ...     LIMIT 1'''
        >>> execute_sql(connection, sql_statement)

    Example:
        >>> from trz_py_utils.db import connect, execute_sql
        >>> import os; uri = os.environ.get("PG_DB_URI");
        >>> sql_statement = '''
        ...     CREATE TABLE IF NOT EXISTS t(col1 VARCHAR);
        ...     INSERT INTO t (col1) VALUES (%s);
        ...     SELECT * FROM t'''
        >>> execute_sql(
        ...     connection=connect(uri),
        ...     sql=sql_statement,
        ...     values=("val1", ),
        ...     is_return=True)
        [('val1',)...
    """
    log.info("executing sql...")
    response = None
    log.debug(f"sql={sql}")
    log.debug(f"values={values}")

    def try_fetch(cursor):
        try:
            return cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            if "no results to fetch" in str(e):
                log.warn(f"{e}")
                return None

    try:
        with connection.cursor() as cursor:
            if isinstance(values, list):
                cursor.executemany(sql, values)
            else:
                cursor.execute(sql, values)
            log.info("executed.")
            if is_return:
                response = try_fetch(cursor)
        connection.commit()
        log.debug("committed.")
    except Exception as e:
        # handle the exception, log it, and then roll back the transaction
        log.error(f"{e}")
        if connection:
            connection.rollback()
        raise e

    log.info("successful SQL execution on db.")

    return response


def add_row(connection: Connection, table: str,
            data: dict[str, Any], **kwargs):
    """Inserts a single record into a postgres database table
    using a dictionary whose keys are column names.

    Args:
        connection (Connection): psycopg db connection (`connect(uri)`)
        table (str): name of the database table to insert into.
        data (dict[str, Any]): dictionary whose keys are column names.

    Returns:
        list[tuple[Any]] | None: db reply if `is_return` arg is truthy.

    Example:
        >>> from trz_py_utils.db import connect, add_row, execute_sql
        >>> import os; uri = os.environ.get("PG_DB_URI")
        >>> conn = connect(uri)
        >>> execute_sql(conn, "CREATE TABLE IF NOT EXISTS t2(col1 VARCHAR);")
        >>> add_row(
        ...     connection=conn,
        ...     table="t2",
        ...     data={"col1": "val1"})
    """
    sql_insert = make_sql_insert_into(table, data)
    values = tuple(data.values())
    log.info(f"adding row to db table '{table}'...")

    return execute_sql(connection, sql_insert, values=values, **kwargs)
