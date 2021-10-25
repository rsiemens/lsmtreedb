from .rbtree import RBTree

MEGABYTE = 1048576


class MaxThresholdExceeded(Exception):
    pass


class MemTable:
    """
    Wrapper around the RBTree that enforces bytes only and has an upperbound on
    how large the tree can grow. When the tree grows past the `max_size_bytes`
    threshold a `MaxThresholdExceeded` exception will be raised.
    """

    def __init__(self, max_size_bytes=MEGABYTE):
        self.max_size_bytes = max_size_bytes
        self.current_size_bytes = 0
        self.rbtree = RBTree()

    def __setitem__(self, key, value):
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)

        additional_bytes = len(key) + len(value)
        if additional_bytes + self.current_size_bytes > self.max_size_bytes:
            raise MaxThresholdExceeded()

        self.rbtree[key] = value

        self.current_size_bytes += additional_bytes

    def __getitem__(self, key):
        assert isinstance(key, bytes)
        return self.rbtree[key]
