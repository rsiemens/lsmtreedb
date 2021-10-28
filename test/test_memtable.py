import pytest

from lsmtree.memtable import BloomFilter, MemTable, SparseIndex


def test_enforce_bytes_only():
    memtable = MemTable(db_dir=".")

    with pytest.raises(AssertionError):
        memtable["key"] = b"foo"  # set key needs to be bytes
    with pytest.raises(AssertionError):
        memtable[b"key"] = "foo"  # set val needs to be bytes
    with pytest.raises(AssertionError):
        memtable["key"]  # get key needs to be bytes

    memtable[b"key"] = b"foo"  # ok


def test_threshold_exceeded(tmp_path):
    memtable = MemTable(db_dir=tmp_path, flush_tree_size=8)

    memtable[b"a"] = b"b"
    assert memtable.current_size_bytes == 2
    memtable[b"bc"] = b"bc"
    assert memtable.current_size_bytes == 6
    memtable[b"d"] = b"d"
    assert memtable.current_size_bytes == 8

    # causes a tree flush since we exceeded max_size_bytes
    memtable[b"e"] = b"e"
    assert memtable.current_size_bytes == 2


def test_flush_tree(tmp_path):
    memtable = MemTable(db_dir=tmp_path)

    for i in [b"b", b"a", b"c"]:
        memtable[i] = i

    assert [kv for kv in memtable.wal] == [(b"b", b"b"), (b"a", b"a"), (b"c", b"c")]
    memtable.flush_tree()
    assert [kv for kv in memtable.wal] == []
    assert len(memtable.rbtree) == 0
    assert memtable.sparse_index is not None
    assert memtable.sparse_index.entries == [(b"a", (0, 36))]
    # all of these keys are in the same block so they share an index
    assert memtable.sparse_index.find(b"a") == (0, 36)
    assert memtable.sparse_index.find(b"b") == (0, 36)
    assert memtable.sparse_index.find(b"c") == (0, 36)


def test_read_from_sparse_index(tmp_path):
    memtable = MemTable(db_dir=tmp_path)

    for i in [b"b", b"a", b"c"]:
        memtable[i] = i

    memtable.flush_tree()
    memtable[b"a"] = b"hello"

    # hits the rbtree
    assert memtable[b"a"] == b"hello"
    # hits the first segment file
    assert memtable[b"c"] == b"c"

    memtable.flush_tree()
    # now hits the new segment file
    assert memtable[b"a"] == b"hello"
    # hits the old segment file
    assert memtable[b"b"] == b"b"
    # not found
    with pytest.raises(KeyError):
        memtable[b"x"]


def test_delete_key(tmp_path):
    memtable = MemTable(db_dir=tmp_path)

    memtable[b"foo"] = b"bar"
    assert memtable[b"foo"] == b"bar"

    # insert a TOMBSTONE in the tree
    del memtable[b"foo"]
    with pytest.raises(KeyError):
        memtable[b"foo"]

    memtable[b"foo"] = b"bar"
    assert memtable[b"foo"] == b"bar"
    memtable.flush_tree()
    # now lives in a segment file. Should still raise cause of the TOMBSTONE
    del memtable[b"foo"]
    with pytest.raises(KeyError):
        memtable[b"foo"]


def test_sparse_index_find():
    index = SparseIndex([("a", 0), ("c", 2), ("e", 4)], segment="fake_path/segment.0")

    assert index.find("a") == 0
    assert index.find("b") == 0
    assert index.find("c") == 2
    assert index.find("d") == 2
    assert index.find("e") == 4
    assert index.find("f") == 4


def test_bloom_filter():
    filter = BloomFilter(size=100, hashes=3)

    assert b"foo" not in filter
    filter.add(b"foo")
    assert b"foo" in filter
    assert not filter._full

    assert b"bar" not in filter
    filter.add(b"bar")
    assert b"bar" in filter
    assert not filter._full

    # everything will collide
    filter = BloomFilter(size=1, hashes=2)
    assert b"foo" not in filter
    filter.add(b"foo")
    assert b"foo" in filter
    assert b"bar" in filter
    assert b"everything_collides" in filter
    assert filter._full
