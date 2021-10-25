import pytest

from lsmtree.memtable import MemTable, SparseIndex


def test_enforce_bytes_only():
    memtable = MemTable(base_dir=".")

    with pytest.raises(AssertionError):
        memtable["key"] = b"foo"  # set key needs to be bytes
    with pytest.raises(AssertionError):
        memtable[b"key"] = "foo"  # set val needs to be bytes
    with pytest.raises(AssertionError):
        memtable["key"]  # get key needs to be bytes

    memtable[b"key"] = b"foo"  # ok


def test_threshold_exceeded(tmp_path):
    memtable = MemTable(base_dir=tmp_path, max_size_bytes=8)

    memtable[b"a"] = b"b"
    assert memtable.current_size_bytes == 2
    memtable[b"bc"] = b"bc"
    assert memtable.current_size_bytes == 6
    memtable[b"d"] = b"d"
    assert memtable.current_size_bytes == 8

    # causes a tree flush since we exceeded max_size_bytes
    memtable[b"e"] = b"e"
    assert memtable.current_size_bytes == 2
    assert memtable.sparse_index_len == 1


def test_flush_tree(tmp_path):
    memtable = MemTable(base_dir=tmp_path)

    for i in [b"b", b"a", b"c"]:
        memtable[i] = i

    memtable.flush_tree()

    assert len(memtable.rbtree) == 0
    assert memtable.sparse_index is not None
    assert memtable.sparse_index.entries == [(b"a", (0, 54))]
    # all of these keys are in the same block so they share an index
    assert memtable.sparse_index.find(b"a") == (0, 54)
    assert memtable.sparse_index.find(b"b") == (0, 54)
    assert memtable.sparse_index.find(b"c") == (0, 54)


def test_read_from_sparse_index(tmp_path):
    memtable = MemTable(base_dir=tmp_path)

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


def test_sparse_index_find():
    index = SparseIndex([("a", 0), ("c", 2), ("e", 4)], segment="fake_path/segment.0")

    assert index.find("a") == 0
    assert index.find("b") == 0
    assert index.find("c") == 2
    assert index.find("d") == 2
    assert index.find("e") == 4
    assert index.find("f") == 4
