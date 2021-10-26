import io
import os
from struct import pack, unpack


class Segment:
    """
    Represents a file segment which contains the keys and values of a tree in
    sorted order.

    Reads and writes a segment file:
    """

    def __init__(self, id, base_dir):
        self.id = id
        self.path = os.path.join(base_dir, f"segment.{self.id}")
        self.file = None

    def open(self):
        if not os.path.exists(self.path):
            self.file = open(self.path, "w+b")
        else:
            self.file = open(self.path, "r+b")

    def close(self):
        self.file.close()

    def read_range(self, start, end=-1):
        self.file.seek(start)

        if end == -1:
            return self.file.read()
        return self.file.read(end - start)

    def write(self, chunk):
        self.file.seek(0, io.SEEK_END)
        written = self.file.write(chunk)
        self.file.flush()
        os.fsync(self.file.fileno())
        return written

    @property
    def tell_eof(self):
        self.file.seek(0, io.SEEK_END)
        return self.file.tell()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MaxSizeExceeded(Exception):
    pass


class Block:
    """
    A block is logical chunk of data in a segment. It contains a simple size
    header then key value pairs. A block is compressible and is  what the sparse
    index points to.

    TODO: add a crc header for corruption checking

    +---------------------------+------------------+-------------+------------------+-------------+
    | 8 bytes block size header | 2 bytes key size | N bytes key | 4 bytes val size | N bytes val |
    +---------------------------+------------------+-------------+------------------+-------------+
    """

    BLOCK_SIZE_FMT = "<Q"  # unsigned 8 byte long long
    KEY_SIZE_FMT = "<H"  # unsigned 2 byte short
    VAL_SIZE_FMT = "<I"  # unsigned 4 byte int

    def __init__(self):
        self.max_size = 2 ** 64
        self.size = 0
        # The first key in the block. Used by the sparse index
        self.key = None
        self.data = []

    def __len__(self):
        return self.size

    def dump(self):
        size = pack(self.BLOCK_SIZE_FMT, self.size)
        return size + b"".join(self.data)

    def add(self, key, value):
        key_len = pack(self.KEY_SIZE_FMT, len(key))
        val_len = pack(self.VAL_SIZE_FMT, len(value))
        record = key_len + key + val_len + value

        if len(record) + self.size > self.max_size:
            raise MaxSizeExceeded("Maximum block size exceeded!")

        self.data.append(record)
        self.size += len(record)

        if self.key is None:
            self.key = key

    @classmethod
    def iter_from_binary(cls, block):
        """
        Iteratively decode key value pairs from a binary block yielding them.
        """
        offset = 8  # skip the block size header bytes
        size = len(block)

        while offset < size:
            key_len = unpack(cls.KEY_SIZE_FMT, block[offset : offset + 2])[0]
            offset += 2
            key = block[offset : offset + key_len]

            offset += key_len
            val_len = unpack(cls.VAL_SIZE_FMT, block[offset : offset + 4])[0]
            offset += 4
            value = block[offset : offset + val_len]
            offset += val_len

            yield key, value
