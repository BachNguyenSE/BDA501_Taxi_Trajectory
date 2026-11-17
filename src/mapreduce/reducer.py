"""Step 1 — COMBINER / REDUCER (Hadoop Streaming contract: key-sorted lines on stdin).

Input  (from mapper or combiner):  key \\t n \\t km \\t seconds
--combiner output (same format, partial sums — so it can run 0..k times):
                                   key \\t n \\t km \\t seconds
reducer output (final, one row per grid cell x hour):
    cell row col hour center_lat center_lon n_segments total_km total_s avg_speed_kmh

Summing (n, km, s) is associative and commutative, which is what makes the combiner
safe; the average speed is computed only at the very end from the summed totals
(averaging averages would weight short and long segments wrongly).
"""
from __future__ import annotations

import argparse
import os
import sys

try:
    from .core import add_param_args, format_output, format_value, group_sorted, params_from_args
except ImportError:  # see mapper.py: shipped -files are importable only via the task working dir
    sys.path.insert(0, os.getcwd())
    from core import add_param_args, format_output, format_value, group_sorted, params_from_args


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--combiner", action="store_true", help="emit partial sums instead of final rows")
    add_param_args(ap)
    args = ap.parse_args(argv)
    p = params_from_args(args)
    out = sys.stdout
    for key, n, km, sec in group_sorted(sys.stdin):
        if args.combiner:
            out.write(f"{key}\t{format_value(n, km, sec)}\n")
        else:
            out.write(format_output(key, n, km, sec, p) + "\n")


if __name__ == "__main__":
    main()
