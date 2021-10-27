import os

from lsmtree.compaction import Compactor
from lsmtree.memtable import MemTable
from lsmtree.segment import Block, Segment


def test_compactor_get_target_segments(tmp_path):
    memtable = MemTable(db_dir=tmp_path)
    for i in range(5):
        with open(os.path.join(tmp_path, f"segment.{i}"), "w+") as f:
            pass

    assert Compactor(memtable).get_target_segments() == [
        0,
        1,
    ]


def test_compactor_iter_kv_pairs(tmp_path):
    memtable = MemTable(tmp_path)
    memtable[b"x"] = b"x"
    memtable[b"a"] = b"a"
    memtable[b"b"] = b"b"
    memtable.flush_tree()

    compactor = Compactor(memtable)
    targets = compactor.get_target_segments()
    results = [kv for kv in compactor.iter_kv_pairs(targets[0])]
    assert results == [(b"a", b"a"), (b"b", b"b"), (b"x", b"x")]


def test_compactor_iter_smallest_kv_pair(tmp_path):
    memtable = MemTable(tmp_path)
    memtable[b"x"] = b"x1"
    memtable[b"a"] = b"a1"
    memtable[b"b"] = b"b1"
    memtable[b"d"] = b"d1"
    memtable[b"y"] = b"y1"
    memtable.flush_tree()

    memtable[b"x"] = b"x2"
    memtable[b"b"] = b"b2"
    memtable[b"c"] = b"c2"
    del memtable[b"d"]
    memtable.flush_tree()

    compactor = Compactor(memtable)
    targets = compactor.get_target_segments()
    results = [kv for kv in compactor.iter_smallest_kv_pair(targets[0], targets[1])]
    expected = [
        (b"a", b"a1"),
        (b"b", b"b2"),
        (b"c", b"c2"),
        (b"d", b""),
        (b"x", b"x2"),
        (b"y", b"y1"),
    ]
    assert results == expected


def test_compactor_compact(tmp_path):
    memtable = MemTable(tmp_path)
    memtable[b"x"] = b"x1"
    memtable[b"a"] = b"a1"
    memtable[b"b"] = b"b1"
    memtable[b"d"] = b"d1"
    memtable[b"y"] = b"y1"
    memtable.flush_tree()

    memtable[b"x"] = b"x2"
    memtable[b"b"] = b"b2"
    memtable[b"c"] = b"c2"
    del memtable[b"d"]
    memtable.flush_tree()

    compactor = Compactor(memtable)
    compactor.compact()
    kvs = []
    with Segment(id=1, db_dir=tmp_path) as segment:
        for _, raw_block in segment:
            for kv in Block.iter_from_binary(raw_block):
                kvs.append(kv)

    expected = [
        (b"a", b"a1"),
        (b"b", b"b2"),
        (b"c", b"c2"),
        (b"x", b"x2"),
        (b"y", b"y1"),
    ]
    assert kvs == expected
