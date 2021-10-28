import os

import pytest

from lsmtree.segment import WAL, Block, MaxSizeExceeded, Segment


def test_segment_write(tmp_path):
    with Segment(id=0, db_dir=tmp_path) as segment:
        written = segment.write(b"hello world!")

    assert os.path.exists(os.path.join(tmp_path, "segment.0"))
    assert written == 12

    with open(segment.path, "rb") as f:
        assert f.read() == b"hello world!"

    # file already exists, should open for append now
    with Segment(id=0, db_dir=tmp_path) as segment:
        segment.write(b" hello again!")

    with open(segment.path, "rb") as f:
        assert f.read() == b"hello world! hello again!"


def test_segment_read(tmp_path):
    # offset, chunk
    with Segment(id=0, db_dir=tmp_path) as segment:
        segment.write(b"abcdefghijk")
        assert segment.read_range(0, 4) == b"abcd"
        assert segment.read_range(4, 7) == b"efg"
        assert segment.read_range(7) == b"hijk"


def test_segment_iter_raw_blocks(tmp_path):
    with Segment(id=0, db_dir=tmp_path) as segment:
        for i in range(5):
            block = Block()
            block.add(b"foo", b"bar")
            segment.write(block.dump())

        count = 0
        for _, raw_block in segment:
            count += 1
            for k, v in Block.iter_from_binary(raw_block):
                assert k == b"foo"
                assert v == b"bar"
        assert count == 5


def test_block_add():
    block = Block()

    block.add(b"foo", b"bar")
    # 2 bytes for key len
    # 3 bytes for the key ("foo")
    # 4 bytes for the val len
    # 3 bytes for the val ("bar")
    first_record_size = 2 + 3 + 4 + 3
    assert len(block) == first_record_size
    assert len(block.data) == 1
    assert block.key == b"foo"

    block.add(b"hello", b"world!")
    second_record_size = 2 + 5 + 4 + 6
    assert len(block) == first_record_size + second_record_size
    assert len(block.data) == 2
    assert block.key == b"foo"

    block.size = 2 ** 64 - 1
    with pytest.raises(MaxSizeExceeded):
        block.add(b"pushed", b"over the edge")


def test_block_dump():
    block = Block()

    block.add(b"foo", b"bar")
    assert (
        block.dump()
        == b"\x00\xef\xd3\xf5\xe2\x0c\x00\x00\x00\x00\x00\x00\x00\x03\x00foo\x03\x00\x00\x00bar"
    )
    assert block.key == b"foo"


def test_block_iter_from_binary():
    block = Block()
    block.add(b"foo", b"bar")
    block.add(b"hello", b"world!")
    block.add(b"key", b"value")
    dump = block.dump()
    decoded = [(k, v) for k, v in Block.iter_from_binary(dump)]

    assert decoded == [(b"foo", b"bar"), (b"hello", b"world!"), (b"key", b"value")]


def test_wal(tmp_path):
    wal = WAL(tmp_path)
    wal.add(b"foo", b"bar")
    wal.add(b"hello", b"world!")

    results = [item for item in wal]
    assert results == [
        (b"foo", b"bar"),
        (b"hello", b"world!"),
    ]

    wal.reset()
    results = [item for item in wal]
    assert results == []
