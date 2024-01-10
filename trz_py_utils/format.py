from typing import Iterable
from decimal import Decimal, ROUND_HALF_UP


def sizeof_fmt(num: int | float, suffix="B"):
    """converts bytes to pretty string, eg. '50KB'

    Args:
        num (int | float): _description_
        suffix (str, optional): _description_. Defaults to "B".

    Returns:
        _type_: _description_

    Example:
        >>> from trz_py_utils.format import sizeof_fmt
        >>> sizeof_fmt(5*1024)
        '5.0KB'

    Example:
        >>> from trz_py_utils.format import sizeof_fmt
        >>> sizeof_fmt(5*1024*1024)
        '5.0MB'
    """
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def unique(iterable: Iterable):
    """Filter out duplicates and respect order.
    `list(set([1, 1, 2, 3, 3, 4]))` would not preserve order.

    Args:
        iterable (Iterable): _description_

    Returns:
        list: unique values in same order.

    Example:
        >>> from trz_py_utils.format import unique
        >>> unique([1, 1, 2, 3, 3, 4])
        [1, 2, 3, 4]

    Example:
        >>> from trz_py_utils.format import unique
        >>> unique(["It", "was", "it", "was"])
        ['It', 'was', 'it']
    """
    values_seen_so_far = []
    for this_value in iterable:
        if this_value not in values_seen_so_far:
            values_seen_so_far.append(this_value)
    return values_seen_so_far


def percent(value, precision='0.00001'):
    return Decimal(value).quantize(
        Decimal(precision),
        rounding=ROUND_HALF_UP)
