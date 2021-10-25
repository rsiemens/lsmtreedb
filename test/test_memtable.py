import pytest

from lsmtree.memtable import MaxThresholdExceeded, MemTable


def test_enforce_bytes_only():
    memtable = MemTable()

    with pytest.raises(AssertionError):
        memtable["key"] = b"foo"  # set key needs to be bytes
    with pytest.raises(AssertionError):
        memtable[b"key"] = "foo"  # set val needs to be bytes
    with pytest.raises(AssertionError):
        memtable["key"]  # get key needs to be bytes

    memtable[b"key"] = b"foo"  # ok


def test_threshold_exceeded():
    memtable = MemTable(max_size_bytes=8)

    memtable[b"a"] = b"b"
    assert memtable.current_size_bytes == 2
    memtable[b"bc"] = b"bc"
    assert memtable.current_size_bytes == 6
    memtable[b"d"] = b"d"
    assert memtable.current_size_bytes == 8

    with pytest.raises(MaxThresholdExceeded):
        memtable[b"e"] = b"e"
