"""
The storage engine basically works like so:

- When a write comes in, add it to an in-memory balanced tree data structure
  (red-black tree). This in-memory tree is called the memtable.
- When the memtable gets bigger than some threshold—typically a few
  megabytes—write it out to disk as an SSTable file. This can be done
  efficiently because the tree already maintains the key-value pairs sorted by
  key. The new SSTable file becomes the most recent segment of the database.
  While the SSTable is being written out to disk, writes can continue to a new
  memtable instance.
- In order to serve a read request, first try to find the key in the memtable,
  then in the most recent on-disk segment, then in the next-older segment, etc.
- From time to time, run a merging and compaction process in the background to
  combine segment files and to discard overwritten or deleted values.

"""
