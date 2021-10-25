import os

import pytest

from lsmtree.segment import Segment


def test_segment_write(tmp_path):
    with Segment(id=0, base_dir=tmp_path) as segment:
        segment.write(b"hello world!")

    assert os.path.exists(os.path.join(tmp_path, "segment.0"))

    with open(segment.path, "rb") as f:
        assert f.read() == b"hello world!"

    # file already exists, should open for append now
    with Segment(id=0, base_dir=tmp_path) as segment:
        segment.write(b" hello again!")

    with open(segment.path, "rb") as f:
        assert f.read() == b"hello world! hello again!"


def test_segment_read(tmp_path):
    # offset, chunk
    with Segment(id=0, base_dir=tmp_path) as segment:
        segment.write(b"abcdefghijk")
        assert segment.read_range(0, 4) == b"abcd"
        assert segment.read_range(4, 7) == b"efg"
        assert segment.read_range(7) == b"hijk"
