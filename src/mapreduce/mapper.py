"""Step 1 — MAPPER (Hadoop Streaming contract: CSV lines on stdin -> "key\\tvalue" on stdout).

One output record per valid 15 s GPS segment:
    <row>-<col>-<hour> \\t 1 \\t <distance_km> \\t <duration_s>

Job counters are reported on stderr as `reporter:counter:<group>,<name>,<n>`, which Hadoop
Streaming turns into real job counters.

Local pipe (same thing Hadoop does, minus the cluster):
    python -m src.mapreduce.mapper < trips.csv | LC_ALL=C sort | python -m src.mapreduce.reducer
On Hadoop:
    hadoop jar hadoop-streaming.jar -files core.py,mapper.py,reducer.py \\
      -mapper "python3 mapper.py" -combiner "python3 reducer.py --combiner" \\
      -reducer "python3 reducer.py" -input /porto/train.csv -output /porto/zone_hour_speed
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

try:  # package import (python -m src.mapreduce.mapper)
    from .core import COUNTER_GROUP, add_param_args, format_value, map_line, params_from_args
except ImportError:
    # Hadoop Streaming symlinks every -files entry into the task's working directory, but
    # Python puts the symlink-RESOLVED script dir on sys.path, and each shipped file lives in
    # its own filecache dir — so core.py is only importable via the working directory.
    sys.path.insert(0, os.getcwd())
    from core import COUNTER_GROUP, add_param_args, format_value, map_line, params_from_args


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_param_args(ap)
    p = params_from_args(ap.parse_args(argv))
    counters: Counter = Counter()
    out = sys.stdout
    for line in sys.stdin:
        for key, km in map_line(line, p, counters):
            out.write(f"{key}\t{format_value(1, km, p.step_s)}\n")
    for name, value in sorted(counters.items()):
        sys.stderr.write(f"reporter:counter:{COUNTER_GROUP},{name},{value}\n")


if __name__ == "__main__":
    main()
