from threading import Lock

from .rbtree import RBTree
from .segment import Block, Segment
from .settings import BLOCK_COMPRESSION, BLOCK_SIZE, RBTREE_FLUSH_SIZE

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

    def __setitem__(self, key, value):
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)

        additional_bytes = len(key) + len(value)
        if additional_bytes + self.current_size_bytes > self.flush_tree_size:
            with self.sparse_index_lock:
                self.flush_tree()
            self.current_size_bytes = 0

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
        self.rbtree[key] = b""

    def find_in_segment_file(self, key):
        sparse_index = self.sparse_index

        while sparse_index:
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
