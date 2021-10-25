import struct

from .rbtree import RBTree
from .segment import Segment

MEGABYTE = 1048576


class MemTable:
    """
    Wrapper around the RBTree that enforces bytes only and has an upperbound on
    how large the tree can grow. When the tree grows past the `max_size_bytes`
    it is flushed to disk and a new RBTree is constructed.
    """

    def __init__(self, base_dir, max_size_bytes=MEGABYTE):
        self.base_dir = base_dir
        self.max_size_bytes = max_size_bytes
        self.current_size_bytes = 0
        self.sparse_index = None
        self.sparse_index_len = 0
        self.rbtree = RBTree()

    def __setitem__(self, key, value):
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)

        additional_bytes = len(key) + len(value)
        if additional_bytes + self.current_size_bytes > self.max_size_bytes:
            self.flush_tree()
            self.current_size_bytes = 0

        self.rbtree[key] = value

        self.current_size_bytes += additional_bytes

    def __getitem__(self, key):
        assert isinstance(key, bytes)
        return self.rbtree[key]

    def flush_tree(self):
        """
        Write the RBtree to disk and build a sparse index that points to offsets
        in the disk. Update the sparse index linked list and then finally
        replace the RBtree with a new one.
        """
        with Segment(self.sparse_index_len, self.base_dir) as segment:
            index = SparseIndex(entries=[], segment=segment.path)

            for k, v in self.rbtree.items():
                key_len = struct.pack("<Q", len(k))
                val_len = struct.pack("<Q", len(v))
                bytes_written = segment.write(key_len + k + val_len + v)
                # TODO: this is being used as a dense index ATM!
                index.add(k, segment.tell_eof - bytes_written)

        if self.sparse_index is None:
            self.sparse_index = index
        else:
            old_head = self.sparse_index
            index.next = old_head
            self.sparse_index = index

        self.sparse_index_len += 1
        self.rbtree = RBTree()


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
