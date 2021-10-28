import io
import os
import zlib
from struct import pack, unpack


class Segment:
    """
    Represents a file segment which contains the keys and values of a tree in
    sorted order.

    Reads and writes a segment file:
    """

    def __init__(self, id, db_dir, fname="segment"):
        self.id = id
        self.path = os.path.join(db_dir, f"{fname}.{self.id}")
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
        cur = self.file.tell()
        self.file.seek(0, io.SEEK_END)
        end = self.file.tell()
        self.file.seek(cur, io.SEEK_SET)
        return end

    def __iter__(self):
        # iterates over the blocks in a segment file
        offset = 0
        size = self.tell_eof

        self.file.seek(0, io.SEEK_SET)
        while offset < size:
            header = self.file.read(Block.HEADER_SIZE)
            flags, _, block_size = unpack(Block.HEADER_FMT, header)
            yield flags, header + self.file.read(block_size)
            offset += Block.HEADER_SIZE + block_size

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MaxSizeExceeded(Exception):
    pass


class BlockCorruption(Exception):
    pass


class Block:
    """
    A block is logical chunk of data in a segment. It contains a simple size
    header, flag header, and then key value pairs. A block is compressible and
    is  what the sparse index points to.

    TODO: add a crc header for corruption checking

    +----------------------+-------------------+---------------------------+------------------+-------------+------------------+-------------+
    | 1 bytes header flags | 4 bytes crc check | 8 bytes block size header | 2 bytes key size | N bytes key | 4 bytes val size | N bytes val |
    +----------------------+-------------------+---------------------------+------------------+-------------+------------------+-------------+
    """

    HEADER_FMT = (
        "<BIQ"  # unsigned 1 byte char, unsigned 4byte int, unsigned 8 byte long long,
    )
    HEADER_SIZE = 13
    KEY_SIZE_FMT = "<H"  # unsigned 2 byte short
    VAL_SIZE_FMT = "<I"  # unsigned 4 byte int
    COMPRESSION_FLAG = 0b10000000

    def __init__(self):
        self.max_size = 2 ** 64
        self.size = 0
        # The first key in the block. Used by the sparse index
        self.key = None
        self.data = []

    def __len__(self):
        return self.size

    def dump(self, compress=False):
        flags = 0b00000000
        data = b"".join(self.data)

        if compress:
            flags |= self.COMPRESSION_FLAG
            data = zlib.compress(data, level=zlib.Z_BEST_SPEED)

        checksum = zlib.crc32(data)
        header = pack(self.HEADER_FMT, flags, checksum, len(data))
        return header + data

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
    def iter_from_binary(cls, block, raise_for_corruption=True):
        """
        Iteratively decode key value pairs from a binary block yielding them.
        """
        flags, checksum, _ = unpack(cls.HEADER_FMT, block[: cls.HEADER_SIZE])
        is_compressed = flags & cls.COMPRESSION_FLAG
        data = block[cls.HEADER_SIZE :]

        if raise_for_corruption and zlib.crc32(data) != checksum:
            raise BlockCorruption()

        if is_compressed:
            data = zlib.decompress(data)

        offset = 0
        size = len(data)
        while offset < size:
            key_len = unpack(cls.KEY_SIZE_FMT, data[offset : offset + 2])[0]
            offset += 2
            key = data[offset : offset + key_len]

            offset += key_len
            val_len = unpack(cls.VAL_SIZE_FMT, data[offset : offset + 4])[0]
            offset += 4
            value = data[offset : offset + val_len]
            offset += val_len

            yield key, value


class WAL:
    """
    The write ahead log for providing durability of the rbtree in case of
    crashes.
    """

    def __init__(self, db_dir):
        self.db_dir = db_dir
        self.segment = Segment(db_dir=db_dir, id="log", fname="wal")

    def add(self, key, value):
        block = Block()
        block.add(key, value)
        with self.segment as segment:
            segment.write(block.dump(compress=False))

    def reset(self):
        os.remove(os.path.join(self.segment.path))
        self.segment = Segment(db_dir=self.db_dir, id="log", fname="wal")

    def __iter__(self):
        with self.segment as segment:
            for _, raw_block in segment:
                for kv in Block.iter_from_binary(raw_block):
                    yield kv
