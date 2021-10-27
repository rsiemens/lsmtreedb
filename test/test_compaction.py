import os

from lsmtree.compaction import Compactor


def test_compactor_get_target_segments(tmp_path):
    for i in range(5):
        with open(os.path.join(tmp_path, f"segment.{i}"), "w+") as f:
            pass

    assert Compactor(db_dir=tmp_path, max_segments_per_run=2).get_target_segments() == [
        0,
        1,
    ]
    assert Compactor(db_dir=tmp_path, max_segments_per_run=4).get_target_segments() == [
        0,
        1,
        2,
        3,
    ]
    assert Compactor(
        db_dir=tmp_path, max_segments_per_run=99
    ).get_target_segments() == [
        0,
        1,
        2,
        3,
        4,
    ]
