import json
from collections import Counter

import pytest

from src.mapreduce.core import Params, format_value, group_sorted, map_line, parse_kv

P = Params()


def csv_line(poly, ts=1372636858, missing="False"):
    return f'"1","C","","","20000589","{ts}","A","{missing}","{json.dumps(poly)}"\n'


def test_map_line_emits_one_record_per_valid_segment():
    c = Counter()
    poly = [[-8.6186, 41.1414], [-8.6185, 41.1414], [-8.6203, 41.1425]]
    out = list(map_line(csv_line(poly), P, c))
    assert len(out) == 2
    assert c["trips_used"] == 1 and c["map_output_records"] == 2


def test_map_line_drops_gps_jump_and_out_of_bbox_segments():
    c = Counter()
    poly = [[-8.6186, 41.1414], [-8.5000, 41.2000], [-9.5, 41.1]]  # ~11 km in 15 s, then outside bbox
    assert list(map_line(csv_line(poly), P, c)) == []
    assert c["segments_gps_jump"] == 1 and c["segments_outside_bbox"] == 1


@pytest.mark.parametrize("line, counter", [
    (csv_line([[-8.6, 41.1], [-8.6, 41.1]], missing="True"), "skip_missing_data"),
    (csv_line([[-8.6, 41.1]]), "skip_lt_2_points"),
    ('"TRIP_ID","CALL_TYPE"\n', "skip_header"),
    ('"1","C","broken\n', "skip_malformed_line"),
])
def test_map_line_skips_bad_records_with_a_counter(line, counter):
    c = Counter()
    assert list(map_line(line, P, c)) == []
    assert c[counter] == 1


def test_cell_of_matches_floor_division_on_grid_lines():
    lon = P.lon_min + 3 * P.grid_deg
    assert P.cell_of(lon, P.lat_min) == (0, 3)


def test_combiner_is_safe_to_apply_any_number_of_times():
    records = [("001-002-08", 1, 0.1, 15), ("001-002-08", 1, 0.2, 15), ("001-003-08", 1, 0.3, 15)]
    lines = [f"{k}\t{format_value(n, km, s)}" for k, n, km, s in records]
    direct = list(group_sorted(lines))
    combined = [f"{k}\t{format_value(n, km, s)}" for k, n, km, s in group_sorted(lines[:2])] + lines[2:]
    after = list(group_sorted(combined))
    # counts and seconds are exact; km only up to the 6-decimal text serialisation (<= 5e-7 per write)
    assert [(k, n, s) for k, n, _, s in after] == [(k, n, s) for k, n, _, s in direct]
    assert [km for _, _, km, _ in after] == pytest.approx([km for _, _, km, _ in direct], abs=1e-6)


def test_reducer_rejects_input_that_was_not_grouped_by_key():
    lines = ["a\t1\t0.1\t15", "b\t1\t0.1\t15", "a\t1\t0.1\t15"]
    with pytest.raises(ValueError):
        list(group_sorted(lines))


def test_parse_kv_roundtrip():
    assert parse_kv("k\t" + format_value(3, 1.5, 45)) == ("k", 3, 1.5, 45)
