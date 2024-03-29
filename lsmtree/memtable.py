import os
import zlib
from threading import Lock

from .rbtree import RBTree
from .segment import WAL, Block, Segment, list_segments
from .settings import (BLOCK_COMPRESSION, BLOCK_SIZE, BLOOM_FILTER_HASHES,
                       BLOOM_FILTER_SIZE, RBTREE_FLUSH_SIZE)

TOMBSTONE = b""


class MemTable:
    """
    Wrapper around the RBTree that enforces bytes only and has an upperbound on
    how large the tree can grow. When the tree grows past the `flush_tree_size`
    it is flushed to disk and a new RBTree is constructed.
    """

    def __init__(self, db_dir, flush_tree_size=RBTREE_FLUSH_SIZE):
        self.db_dir = db_dir
        self.flush_tree_size = flush_tree_size
        self.current_size_bytes = 0
        # An improvement here could be a RWLock instead of simple mutex if
        # we want to allow concurrent reads in the future
        self.sparse_index_lock = Lock()
        self.sparse_index = None
        self.sparse_index_counter = 0
        self.rbtree = RBTree()
        self.wal = WAL(db_dir)

    def __setitem__(self, key, value):
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)

        additional_bytes = len(key) + len(value)
        if additional_bytes + self.current_size_bytes > self.flush_tree_size:
            with self.sparse_index_lock:
                self.flush_tree()
            self.current_size_bytes = 0

        self.wal.add(key, value)
        self.rbtree[key] = value

        self.current_size_bytes += additional_bytes

    def __getitem__(self, key):
        assert isinstance(key, bytes)
        try:
            val = self.rbtree[key]
        except KeyError:
            with self.sparse_index_lock:
                val = self.find_in_segment_file(key)

        # value hasn't yet been cleaned up by compaction
        if val == TOMBSTONE:
            raise KeyError(key)

        return val

    def __delitem__(self, key):
        assert isinstance(key, bytes)
        self.wal.add(key, TOMBSTONE)
        self.rbtree[key] = TOMBSTONE

    def find_in_segment_file(self, key):
        sparse_index = self.sparse_index

        while sparse_index:
            if key not in sparse_index.bloomfilter:
                sparse_index = sparse_index.next
                continue

            start, end = sparse_index.find(key)
            with Segment(id=sparse_index.segment, db_dir=self.db_dir) as segment:
                block = segment.read_range(start, end)
                val = self.find_in_block(key, block)

                if val is not None:
                    return val
                else:
                    sparse_index = sparse_index.next

        raise KeyError(key)

    def find_in_block(self, key, raw_block):
        for k, v in Block.iter_from_binary(raw_block):
            if k == key:
                return v
        return None

    def flush_tree(self):
        """
        Write the RBtree to disk and build a sparse index that points to offsets
        in the disk. Update the sparse index linked list and then finally
        replace the RBtree with a new one.
        """
        with Segment(self.sparse_index_counter, self.db_dir) as segment:
            index = SparseIndex(entries=[], segment=segment.id)
            block = Block()

            for key, val in self.rbtree.items():
                block.add(key, val)
                index.bloomfilter.add(key)

                if len(block) > BLOCK_SIZE:
                    bytes_written = segment.write(
                        block.dump(compress=BLOCK_COMPRESSION)
                    )
                    eof_pos = segment.tell_eof
                    index.add(block.key, (eof_pos - bytes_written, eof_pos))
                    block = Block()

            # write whatever is left
            if block.data:
                bytes_written = segment.write(block.dump(compress=BLOCK_COMPRESSION))
                index.add(
                    block.key, (segment.tell_eof - bytes_written, segment.tell_eof)
                )

        if self.sparse_index is None:
            self.sparse_index = index
        else:
            old_head = self.sparse_index
            index.next = old_head
            self.sparse_index = index

        self.sparse_index_counter += 1
        self.rbtree = RBTree()
        self.wal.reset()

    @classmethod
    def reconstruct(cls, db_dir):
        # Check the last segment file for corruption in case we crashed mid
        # write. If so discard the file
        # Rebuild from WAL
        memtable = cls(db_dir)
        segment_ids = sorted(list_segments(db_dir))

        # rebuild the sparse index
        for segment_id in segment_ids:
            with Segment(id=segment_id, db_dir=db_dir) as segment:
                index = SparseIndex(entries=[], segment=segment_id)
                corrupted = False

                for offset, _, size, block in segment:
                    if Block.is_block_corrupted(block) and segment_id != max(
                        segment_ids
                    ):
                        raise Exception(f"Corruption on {segment_id} - unrecoverable")
                    elif Block.is_block_corrupted(block) and segment_id == max(
                        segment_ids
                    ):
                        # The WAL will rebuild this
                        corrupted = True
                        break

                    first_key = None
                    for k, v in Block.iter_from_binary(block):
                        if first_key is None:
                            first_key = k
                        index.bloomfilter.add(k)

                    index.add(first_key, (offset, offset + size + Block.HEADER_SIZE))

            if corrupted:
                print("Segment corrupted, removing")
                segment.remove()
            else:
                if memtable.sparse_index is None:
                    memtable.sparse_index = index
                else:
                    old_index = memtable.sparse_index
                    memtable.sparse_index = index
                    index.next = old_index
                memtable.sparse_index_counter = memtable.sparse_index.segment + 1

        for k, v in memtable.wal:
            memtable.rbtree[k] = v

        return memtable


class SparseIndex:
    """
    The sparse index is a ordered list of `(key, byte_offsets)` tuples in a
    segment file. It's sorted so that a binary search can be performed on it.
    It's possible a key doesn't exist in the sparse index, but fall into a range
    between two other keys. In that case the lower of the two keys is taken.

    The sparse index forms a linked list where the head is always the newest
    segment file. That way when searching for a key if it's not found in the
    first segment file we can get the next sparse index + segment file and
    check there.
    """

    def __init__(self, entries, segment, sort=True):
        self.entries = entries
        self.segment = segment
        self.next = None
        self.bloomfilter = BloomFilter(
            size=BLOOM_FILTER_SIZE, hashes=BLOOM_FILTER_HASHES
        )

        if sort:
            self.sort()

    def sort(self):
        self.entries = sorted(self.entries, key=lambda t: t[0])

    def add(self, key, offset):
        self.entries.append((key, offset))

    def find(self, key):
        low = 0
        high = len(self.entries) - 1

        while low <= high:
            middle = low + (high - low) // 2
            entry = self.entries[middle]

            if key < entry[0]:
                high = middle - 1
            elif key > entry[0]:
                low = middle + 1
            else:
                return entry[1]

        return self.entries[high][1]


class BloomFilter:
    def __init__(self, size, hashes):
        # This is very simple. A bit array would be nicer and more memory friendly.
        self.size = size
        self.hashes = hashes
        self._filter = [False] * size
        self._full = False

    def _get_hashed_indexes(self, item):
        indexes = []
        for i in range(self.hashes):
            hash = zlib.crc32(item + bytes(i))
            indexes.append(hash % self.size)
        return indexes

    def __contains__(self, item):
        # save the cost of doing some hash checks
        if self._full:
            return True

        indexes = self._get_hashed_indexes(item)
        return all([self._filter[i] for i in indexes])

    def add(self, item):
        if self._full:
            return

        for index in self._get_hashed_indexes(item):
            self._filter[index] = True

        if all(self._filter):
            self._full = True
