# The size, in bytes, that the red black tree can grow until it should be
# flushed to disk. Note it's possible to go over this limit (ex. The size is
# just below the flush size and the last insert puts it over.) This is to allow
# a single large insert over the flush size to not prevent it from being written.
#
# A higher value here provide faster write and read through put, but requires
# greater memory requirements since the tree needs to be stored in memory. Also
# the larger the value the more time that will be spent blocking when the disk
# is flushed to disk, but how often a disk flush happens will be reduced,
# resulting in fewer, but larger segment files.
RBTREE_FLUSH_SIZE = 1048576 * 3  # 3 MB

# The size, in bytes, that a block can grow until a new one is started.
# Similarly with the red black tree it is possible to go over this limit.
#
# A higher value here means the sparse index is less dense, and less writes to
# disk occur, but read time will be slightly reduced since the block can hold
# more key value pairs in the sparse index gaps.
BLOCK_SIZE = 1024 * 10  # 10 KB

# Should blocks in a segment file be compressed with zlib compression? The
# trade-off here is slightly slower write throughput for increased storage
# efficiency.
BLOCK_COMPRESSION = True


# Larger number for less chance of collision and better read performance, but
# increased memory usage.
BLOOM_FILTER_SIZE = 9679  # prime number
BLOOM_FILTER_HASHES = 3
