import logging
from typing import Iterator
from uuid import uuid4
from charset_normalizer import from_path
import chardet
import subprocess
from tqdm import tqdm
import re
from re import Match
import json

from .format import percent


log = logging.getLogger(__name__)


def read_file(path: str):
    """returns string contents of file given filepath.

    Args:
        path (str): filepath to read contents of

    Returns:
        str: contents of file

    Example:
        >>> from trz_py_utils.file import read_file
        >>> path = "/tmp/asdlkjasdlkajsd"
        >>> with open(path, 'w') as f:
        ...     f.write("hello world!")
        12
        >>> read_file(path)
        'hello world!'
    """
    log.info(f"loading file '{path}'...")
    with open(path, 'r') as f:
        return f.read()


def remove_crlf(src: str, dest=None):
    dest = dest or f"/tmp/{uuid4()}"
    with open(src, 'r', newline='\n') as fi:
        with open(dest, 'w', newline='\n') as fo:
            fo.write(fi.read())


def replace(src: str, s: str, r: str, dest=None):
    dest = dest or f"/tmp/{uuid4()}"
    with open(src, "r") as fi:
        with open(dest, "w", newline="\n") as fo:
            for li in fi.readlines():
                fo.write(li.replace(s, r) if s in li else li)
    return dest


def utf8_encode(src: str, dest=None):
    dest = dest or f"{src}.utf8"
    with open(dest, "w") as fo:
        fo.write(str(from_path(src).best()))

    return dest


class BadLine:
    """Handles operations on lines containing non-utf8 characters.
    """
    def __init__(self,
                 path: str,
                 error: UnicodeDecodeError = None,
                 re_matches: Iterator[Match[str]] = None,
                 line: str = None,
                 line_no: int = None):
        # self.position = file.tell()
        self.line_no = line_no
        self.position = self._i_pos_from_error(error)
        self.path = path
        self.encoding = None
        self.re_matches = re_matches
        self.line = line

    def peak_bad_char(self, offset=4, mode="r"):
        position_start = int(self.position) - offset
        position_end = self._seek_until_newline(position_start)
        with open(self.path, mode) as file:
            file.seek(position_start)
            rest = file.read(position_end - position_start)
            log.info(rest)
            log.info(f"{' '*(offset+2)}^")

    def _seek_until_newline(self, position: int):
        with open(self.path, 'r') as file:
            file.seek(position)
            while True:
                char = file.read(1)
                if not char or char == '\n':
                    break
            return file.tell()

    def _substr_between(self, s: str, start: str, end: str):
        start_index = s.find(start)
        end_index = s.find(end, start_index + len(start))

        if start_index != -1 and end_index != -1:
            return s[start_index + len(start):end_index]

    def _i_pos_from_error(self, e: UnicodeDecodeError):
        if not e:
            return None
        if "position" not in str(e):
            log.error("failed to get byte position from error")
            return None
        position = self._substr_between(str(e), "position ", ": ")
        log.info(f"found bad character position: {position}")
        return int(position)

    def caret_under_matches(self):
        caret_line = [' '] * len(self.line)
        preface = f"line {self.line_no}: "
        pl = len(preface)

        for m in self.re_matches:
            start, end = m.span()
            caret_line[start+pl+6:end+pl+6] = ['^'] * (end - start)

        return preface+self.line+"\n"+''.join(caret_line)


class BadFileReader:
    """Handles detection and conversion of files with non-utf8 characters.

    Example:
        >>> from trz_py_utils.file import BadFileReader
        >>> src = '/tmp/example'
        >>> with open(src, "w") as f:
        ...     _ = f.write("1")
        ...     _ = f.write("09BB¿~NY~1G")
        ...     _ = f.write("ASDKLJ~NULL~1G")
        ...     _ = f.write("2")
        >>> bfr = BadFileReader(src)
        >>> bfr.read(mode="r", encoding="latin-1")

        .. image:: ./_static/bfr-read-example.png
           :target: ../public/index.html
    """
    def __init__(self, path: str, mode="r", i_pos=None,
                 replace="~NULL~", replace_with="~~", **kwargs):
        self.i_pos = i_pos
        self._mode = mode
        self.path = path
        self._i = 0
        self.bad_lines: list[BadLine] = []
        self.lines: list[str] = []
        self.re_bad = r"[^\x00-\x7F]"
        self.replace = replace
        self.replace_with = replace_with
        self._kwargs = kwargs or {}

    def encoding_chardet(self, mode="rb"):
        mode = mode or self._mode
        with open(self.path, mode) as file:
            result = chardet.detect(file.read())
        return result['encoding']

    def encoding(self, bytes_scan_encoding: int = 1024*1024):
        """Uses subshell command `file` with `-i` option to get encoding.

        Args:
            bytes_scan_encoding (int, optional): number of bytes
                    to scan in the file before deciding what encoding it is.
                    Defaults to 1024*1024.

        Raises:
            ChildProcessError: If status code not 0 and stderr exists.

        Returns:
            str | None: encoding of the file.
        """
        b = bytes_scan_encoding
        cmd = f"file -v; file '{self.path}' -i -P encoding=${b}"
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True)
            log.info(result.stdout)
            if result.stderr and result.returncode != 0:
                raise ChildProcessError(result.stderr)
        except Exception as e:
            log.error(f"ERROR: failed to run `file` in subshell:\n{e}")
            return None

        encoding = result.stdout.split("charset=")[-1].strip()
        if encoding == "us-ascii":
            log.warn("'us-ascii' won't work with `open()`, use 'latin-1'")

        return encoding
        # return result.stdout

    def _yield_lines(self, **kwargs):
        kwargs["mode"] = kwargs.get("mode", self._mode)
        log.info(f"using options for read():\n{json.dumps(kwargs, indent=4)}")
        with open(self.path, **kwargs) as file:
            opts = {
                "total": self.num_total,
                "unit": 'line',
                "desc": "reading lines",
            }
            with tqdm(**opts) as progress_bar:
                while True:
                    try:
                        self._i += 1
                        progress_bar.update(1)
                        good_line = self._parse_line(
                            line=next(file).strip(), file=file)
                        if good_line:
                            yield good_line
                    except StopIteration:
                        break  # End of file reached
                    except UnicodeDecodeError as e:
                        print(f"ERROR: line {self._i}: {e}")
                        self.bad_lines.append(BadLine(file, e))
                    except Exception as e:
                        print(f"ERROR: line {self._i}: {e}")

    def _parse_line(self, line: str, file):
        # filter out lines containing non-ascii characters
        matches_bad = list(re.finditer(self.re_bad, line))
        if matches_bad:
            bl = BadLine(path=file.name,
                         error=None,
                         re_matches=matches_bad,
                         line=line,
                         line_no=self._i)
            log.info(bl.caret_under_matches())
            self.bad_lines.append(bl)
            return None

        # replace substring if found
        if self.replace in line:
            line = line.replace(self.replace, self.replace_with)

        return line

    def read(self, **kwargs):
        """Read in a file. Sorts good and bad lines based on
        regex and UnicodeDecodeError. Replaces '~NULL~'
        with '~~' by default.

        Arguments:
            kwargs (optional): any arguments for `open()`.

        Example:
            >>> from trz_py_utils.file import BadFileReader
            >>> src = '/tmp/example'
            >>> with open(src, "w") as f:
            ...     _ = f.write("hello world!")
            >>> bfr = BadFileReader(src)
            >>> bfr.read(mode="r", encoding="latin-1")
            >>> bfr.num_total
            1
            >>> bfr.num_good
            1
            >>> bfr.num_bad
            0

        Example:
            >>> from trz_py_utils.file import BadFileReader
            >>> src = '/tmp/example'
            >>> with open(src, "w") as f:
            ...     _ = f.write("09BB¿~NY~1G")
            >>> bfr = BadFileReader(src)
            >>> bfr.read(mode="r", encoding="latin-1")
            >>> bfr.num_bad
            1
        """
        self.bad_lines = []
        self.lines = []
        log.info("counting lines...")
        self.num_total = sum(1 for _ in open(self.path, **kwargs))
        log.info(f"{self.num_total} total lines.")
        for line in self._yield_lines(**kwargs):
            self.lines.append(line)
        self.num_good = len(self.lines)
        self.num_bad = len(self.bad_lines)
        log.info(f"found {self.num_bad} bad lines ()")

    def is_equal_columns_every_line(self, sep="~"):
        bad_lines = []
        bad_lines_i = []
        with open(self.path, self._mode) as fi:
            n = fi.readline().count(sep)
            i = 2
            for li in fi.readlines():
                c = li.count(sep)
                if c != n:
                    log.info(f"! line {i+1} has {c} when header has {n}")
                    bad_lines.append(li)
                    bad_lines_i.append(i)
                i += 1
        return len(bad_lines_i) < 1

    def write(self, dest: str = f"/tmp/{uuid4()}",
              lines: list[str] = None, **kwargs):
        """Write the good lines out to a new file with \n newlines.

        Args:
            dest (_type_, optional): _description_. Defaults
            to f"/tmp/{uuid4()}".
            lines (list[str], optional): lines to write. Defaults
            to self.good_lines.
            kwargs (optional): any arguments to pass to open() function.

        Returns:
            str: path to output file

        Example:
            >>> from trz_py_utils.file import BadFileReader
            >>> src = '/tmp/example'
            >>> with open(src, "w") as f:
            ...     _ = f.write("hello world!")
            >>> bfr = BadFileReader(src)
            >>> bfr.read(mode="r", encoding="latin-1")
            >>> bfr.write(dest="/tmp/myfile", lines=["newline"])
            '/tmp/myfile'
        """
        log.info("calculating satistics...")
        kwargs["mode"] = kwargs.get("mode", "w")

        n_lines = len(lines) if lines else self.num_good
        n_total = self.num_total
        lines = lines or self.lines
        n_bad = n_total - n_lines
        pct = percent(100*n_lines / n_total)

        log.info(f"writing '{dest}'")
        log.info(f"{n_lines} lines ({n_total} - {n_bad}) ({pct}%)")
        log.info(f"with options:\n{json.dumps(kwargs, indent=4)}")

        if kwargs.pop("dry_run", ""):
            log.info("skipping write (dry_run=True)")
            return dest

        opts = {
            "total": n_lines,
            "unit": 'line',
            # "unit_scale": True,
            "desc": "writing lines",
        }
        with tqdm(**opts) as progress_bar:
            with open(dest, **kwargs) as fo:
                for line in lines:
                    fo.write(f"{line}\n")
                    progress_bar.update(1)

        return dest

    def print_bad_lines(self):
        """Show highlight character positions in each bad line.

        Example:
            >>> from trz_py_utils.file import BadFileReader
            >>> src = '/tmp/example'
            >>> with open(src, "w") as f:
            ...     _ = f.write("09BB¿~NY~1G")
            >>> bfr = BadFileReader(src)
            >>> bfr.read(mode="r", encoding="latin-1")
            >>> lines = bfr.print_bad_lines()
            >>> print(lines[0])
            line 1: 09BBÂ¿~NY~1G
                        ^^
        """
        lines = []
        for bl in self.bad_lines:
            line = bl.caret_under_matches()
            lines.append(line)
            log.info(line)
        return lines
