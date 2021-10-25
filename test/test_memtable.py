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
    assert memtable.sparse_index.find(b"a") == 0
    # 8bytes for keylen + key + 8bytes for vallen + val
    # 8 + 1 + 8 + 1
    assert memtable.sparse_index.find(b"b") == 18
    assert memtable.sparse_index.find(b"c") == 36


def test_sparse_index_find():
    index = SparseIndex([("a", 0), ("c", 2), ("e", 4)], segment="fake_path/segment.0")

    assert index.find("a") == 0
    assert index.find("b") == 0
    assert index.find("c") == 2
    assert index.find("d") == 2
    assert index.find("e") == 4
    assert index.find("f") == 4
