import time
import logging as log
from tabulate import tabulate


class Stopwatch:
    """This class keeps track of named stopwatches for timing sections of code.
    """
    def __init__(self):
        self.starts = {}
        self.stops = {}
        self.elapseds = {}  # seconds elapsed by name
        self.counts = {}  # row counts by name
        self.watch_counts = {}  # map seconds elapsed to row counts

    def print(self, **kwargs):
        log.info("[Stopwatch]"+"".join([f"[{v}]" for v in kwargs.values()]))

    def start(self, name=None):
        name = name or f"timer_{len(self.starts)}"
        self.starts[name] = time.time()

    def stop(self, name=None, count=None, count_name=None):
        name = name or f"timer_{len(self.starts)-1}"
        self.stops[name] = time.time()

        elapsed = self.stops[name] - self.starts[name]
        if name in self.elapseds:
            self.elapseds[name] += elapsed
        else:
            self.elapseds[name] = elapsed
        self.elapseds[name] = round(self.elapseds[name], 1)

        if count is not None:
            count_name = count_name or name
            self.counts[count_name] = {name: int(count)}
            elapsed = self.elapseds[name]
            count = self.counts[count_name][name]
            self.print(watch=name, msg=f"elapsed={elapsed}, count={count:,}")
        else:
            self.print(watch=name, msg=f"elapsed={self.elapseds[name]}")

        return self.elapseds[name]

    def summary_string(self, names: list = [], timers: dict = {}, decimals=1):
        if len(names) > 0:
            timers = {k: v for k, v in self.elapseds.items() if k in names}
        lines = [f"{k}: \t\t{int(v):,}" for k, v in timers.items()]
        return "\t"+"\n\t".join(lines)

    def search(self, watch=None, count=None):
        if watch is not None:
            return {k: v for k, v in self.elapseds.items() if watch in k}
        elif count is not None:
            return {k: v for k, v in self.counts.items() if count in k}

    def count(self, watch_name, count_name, count):
        self.counts[count_name] = {watch_name: int(count)}

    def _stats_row(self, count_name=None, watch_name=None):
        """create row in summary table for combo of count_watch name."""
        try:
            if count_name is not None and watch_name is not None:
                count = self.counts[count_name][watch_name]
            elif watch_name is not None:
                count = -1
                count_name = "None"
            else:
                msg = "(name of counter and name of watch) OR name of watch"
                raise ValueError(f"must specify either: {msg}")
            elapsed = round(self.elapseds[watch_name], 2)
            speed = int(count/elapsed) if elapsed else 0
        except KeyError:
            self.print(msg=f"ERROR failed to find watch '{watch_name}'")
            count = elapsed = speed = -1
        finally:
            return (watch_name,
                    count_name,
                    f"{count:,}",
                    f"{elapsed:,}",
                    f"{speed:,}")

    def make_stats_table(self, count_names=[], watch_names=[],
                         sort_by="metric name"):
        """print summary of counts, elapsed seconds, and rows-per-second."""
        print_rows = []
        n_counts = len(count_names)
        n_watches = len(watch_names)
        # filter for count names AND watch names
        if n_counts > 0 and n_watches > 0:
            for count_name in [n for n in count_names if n in self.counts]:
                counts = self.counts[count_name]
                for watch_name in [n for n in watch_names if n in counts]:
                    print_rows.append(self._stats_row(count_name, watch_name))
        # if no filter specified and stopwatch has counts, print them all
        elif n_counts == 0 and n_watches == 0 and len(self.counts) > 0:
            for count_name in self.counts:
                for watch_name in self.counts[count_name]:
                    print_rows.append(self._stats_row(count_name, watch_name))
        # if no filter and stopwatch has no counts, print any/all watch names
        elif n_counts == 0 and n_watches == 0 and len(self.elapseds) > 0:
            for watch_name in self.elapseds:
                print_rows.append(self._stats_row(watch_name=watch_name))

        return tabulate(print_rows,
                        headers=[
                            "metric name",
                            "count name",
                            "count",
                            "elapsed (s)",
                            "speed (rows/s)"],
                        tablefmt="fancy_grid")

    def print_summary(self, count_names=[],
                      watch_names=[], sort_by="metric name", **kwargs):
        """_summary_

        Args:
            count_names (list, optional): _description_. Defaults to [].
            watch_names (list, optional): _description_. Defaults to [].
            sort_by (str, optional): _description_. Defaults to "metric name".

        Example:
            >>> # row counting example with summary printout
            >>> WATCH = Stopwatch()
            >>> WATCH.start("my count")
            >>> time.sleep(1)
            >>> WATCH.stop("my count", count=12345)
            1.0
            >>> print(WATCH.print_summary())
            ╒═══════════════╤══════════════╤═════════╤═══════════════╤══════════════════╕
            │ metric name   │ count name   │ count   │   elapsed (s) │ speed (rows/s)   │
            ╞═══════════════╪══════════════╪═════════╪═══════════════╪══════════════════╡
            │ my count      │ my count     │ 12,345  │             1 │ 12,345           │
            ╘═══════════════╧══════════════╧═════════╧═══════════════╧══════════════════╛

            >>> # time a single operation with multiple row counts
            >>> _metric_name = "my many-count timer"
            >>> WATCH = Stopwatch()
            >>> WATCH.start(_metric_name)
            >>> time.sleep(1)
            >>> WATCH.stop(_metric_name)
            1.0
            >>> WATCH.count(_metric_name, "my counter 1", 12345)
            >>> WATCH.count(_metric_name, "my counter 2", 23456)
            >>> print(WATCH.print_summary())
            ╒═════════════════════╤══════════════╤═════════╤═══════════════╤══════════════════╕
            │ metric name         │ count name   │ count   │   elapsed (s) │ speed (rows/s)   │
            ╞═════════════════════╪══════════════╪═════════╪═══════════════╪══════════════════╡
            │ my many-count timer │ my counter 1 │ 12,345  │             1 │ 12,345           │
            ├─────────────────────┼──────────────┼─────────┼───────────────┼──────────────────┤
            │ my many-count timer │ my counter 2 │ 23,456  │             1 │ 23,456           │
            ╘═════════════════════╧══════════════╧═════════╧═══════════════╧══════════════════╛
        """  # noqa
        table_str = self.make_stats_table(count_names, watch_names, sort_by)
        log.info(table_str)
        return table_str
