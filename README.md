# Introduction
This was a small project to learn more about SSTables and
[LSM-Trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree) which is the storage engine powering many key value
based storage engines like [RocksDB](https://rocksdb.org/) and [LevelDB](https://github.com/google/leveldb).

[Designing Data-Intensive Applications](https://dataintensive.net/) chapter 3 (Storage and Retrieval) has a really good
overview of the overall structure I am trying to achieve here. [Database Internals](https://www.databass.dev/) also has
a section on it.

The main goal here was to learn more about database engines by doing!

# Implementation Details

![](https://cdn.zappy.app/afcc77fe267abfb9dda1840b078c7c86.png)

The database consists of just a couple components which work in conjunction to serve `get`, `set`, and `delete`
operations. The system works like this:

`set` operations are applied against a `MemTable` instance. The keys and values associated with those
operations are first applied to a write ahead log (WAL) and then applied to a red black tree. The WAL serves the purpose 
of being able to recover the red black tree in case of the database crashing or being brought down.

When the red black tree fills up to a predefined size (a couple of MB) then the tree is flushed in key sorted order into
a series of blocks. Each block is a stand-alone disk write (each a few KB) to a segment file. The block is zlib
compressed and stamped with a checksum for data corruption checking. When the block is written to the segment file, the
first key in the block is remembered and stored in a sparse index. When the segment file is done being written then the
WAL is reset.

There is a sparse index for each segment file. The sparse index is simply a list of `key -> offset` pairs, where the
offset points to the start of a block in the segment file (remember all the keys are in sorted order on disk). This
allows performing a binary search on the sparse index to find a key. For example, if we have key `A` and `M` in the
sparse index, and we wanted to find key `C`, then we would load the block pointed to by `A` and search it for `C`.

When a `get` operation occurs we first check the red black tree to see if it contains the value. If it does, return it.
Otherwise look at the latest sparse index, find the block corresponding to the key, load it from disk and search it for
the key. If it is found return it. If it is not found look into the next sparse index which corresponds with an older
segment file. This is repeated until the key is found or all sparse index + segment file pairs are exhausted at which
point we consider it a non existent key.

Because the segment files are immutable and you will eventually end up with older segments which contain stale key,
value pairs. For example, at one point in time a `set a=123` was applied which was eventually flushed to `segment.1`.
Then later a `set a=456` is applied and flushed to `segment.2`. A search for key `a` will end at the latest `segment.2`
file and the old `segment.1` `a` entry now just takes up space. Enter compaction.

Compaction is a background thread that looks at the oldest couple of segment files and merges them together. It takes
advantage of two important facts.

1. Segments files store keys in sorted order
2. `segment.1` is older than `segment.2`

With these facts a very simple algorithm can be done to merge segment files together while only reading / writing 
a block of data at a time. It works like this:

1. Read a block of data from both `segment.1` and `segment.2`
2. Get the first key value pair from each block.
3. Take the smaller key and apply it to a new segment (`_compact_segment.2`). Keep the larger key for the next round of
   comparisons. If the keys are the same then take the key from the newer segment and discard the older one.
4. Repeat for all keys in the segment until no more keys are left all the segments.
5. Move the `_compact_segment.2` file to `segment.2` and remove the `segment.1` file.
6. Update the sparse index in the `MemTable`.

Steps 5 and 6 acquire a lock on the sparse index to prevent race conditions where a read tries to use an old sparse
index into the newly merged segment file. Compaction is repeated on all segment files until there is only one remaining.

# Benchmarking

A very rudimentary benchmark / test can be run with `python simple_bench.py`.  To run the benchmark you need to first
generate a dataset to work with.

An example of generating a dataset and running the benchmark:
```
poetry install
poetry run python generate_example_dataset.py 100000
poetry run python simple_bench.py
```

`simple_bench.py` takes the `--no-compaction` flag to disable background compaction of segment files. This mostly serves
to show how important compaction of segment files is with regards to the read performance of the database.

Example output based on `poetry run python generate_example_dataset.py 200000`

Without compaction - slightly faster writes, much slower read and larger overall db size
```
poetry run python simple_bench.py --no-compaction
...
5837.06 writes/sec (300000 total in 51.40 sec)
avg  : 0.00017131922642389934
min  : 4.291534423828125e-05
max  : 0.10053396224975586
median: 0.00020313262939453125
stddev: 0.000711248251460834

1404.91 reads/sec (200000 total in 142.36 sec)
avg  : 0.0007117910695075989
min  : 7.152557373046875e-07
max  : 0.06967616081237793
median: 0.0005660057067871094
stddev: 0.0006129092222484116

2.83MB tree size
29.77MB DB size
21 segments
	311 blocks in segment.20
	311 blocks in segment.19
	311 blocks in segment.18
	312 blocks in segment.17
	312 blocks in segment.16
	311 blocks in segment.15
	312 blocks in segment.14
	312 blocks in segment.13
	312 blocks in segment.12
	312 blocks in segment.11
	312 blocks in segment.10
	312 blocks in segment.9
	312 blocks in segment.8
	312 blocks in segment.7
	312 blocks in segment.6
	313 blocks in segment.5
	312 blocks in segment.4
	312 blocks in segment.3
	312 blocks in segment.2
	312 blocks in segment.1
	312 blocks in segment.0
```

With compaction - slightly slower writes, faster reads and reduced overall database size (dependent on updates being performed)
```
poetry run python simple_bench.py
...
5710.67 writes/sec (300000 total in 52.53 sec)
avg  : 0.00017511073509852092
min  : 4.57763671875e-05
max  : 0.11782073974609375
median: 0.00020003318786621094
stddev: 0.0007539583985681953

14231.53 reads/sec (200000 total in 14.05 sec)
avg  : 7.026651740074158e-05
min  : 0.0
max  : 0.02262711524963379
median: 7.224082946777344e-05
stddev: 5.7322595581511305e-05

2.83MB tree size
21.72MB DB size
1 segments
	4591 blocks in segment.20
```


# Conclusion

This was an incredibly fun exercise that taught me a lot and removed much of the "magic" behind how write heavy NoSQL
style databases are implemented. Of course this was just the bare basics and a production key value databases engine 
provide tons of additional features layered on top like range queries, secondary indexes, multi object transactions,
concurrent readers, etc. Also, while I implemented this in python, given more time, I would like to attempt porting it
to a systems language like rust of C.
