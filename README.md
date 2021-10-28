This is a hackathon project to learn more about SSTables and LSM-Trees which is
the storage engine powering many key value based storage engines like RocksDB and
LevelDB.

Designing Data-Intensive Applications chapter 3 (Storage and Retrieval) has a really good overview of the overall
structure I am trying to achieve here. 

The main goal here is to learn more about database engines by doing!

A very rudimentary benchmark / test can be run with `python simple_bench.py`
Running the benchmark:

```
poetry install
poetry run python generate_example_dataset.py 100000
poetry run python simple_bench.py
```

`simple_bench.py` takes the `--no-compaction` flag to disable background compaction of segment files.
Enabling it should greatly reduce read times and over all file database size with only a slight hit
to writes/sec.

Example based on `poetry run python generate_example_dataset.py 200000`

Without compaction - Slightly faster writes, much slower read and larger overall db size
```
poetry run python simple_bench.py --no-compaction
4799.69 writes/sec (300000 total in 62.50 sec)
avg  : 0.0002083469231923421
min  : 4.38690185546875e-05
max  : 0.09340071678161621
median: 0.00020694732666015625
stddev: 0.0006979012059949072

1460.64 reads/sec (200000 total in 136.93 sec)
avg  : 0.0006846308124065399
min  : 7.152557373046875e-07
max  : 0.03731799125671387
median: 0.0005471706390380859
stddev: 0.0005571742040829339

2.83MB tree size
29.77MB db size
21 segments
	311 blocks in segment.20
	311 blocks in segment.19
	311 blocks in segment.18
	311 blocks in segment.17
	311 blocks in segment.16
	311 blocks in segment.15
	312 blocks in segment.14
	313 blocks in segment.13
	312 blocks in segment.12
	312 blocks in segment.11
	312 blocks in segment.10
	312 blocks in segment.9
	313 blocks in segment.8
	312 blocks in segment.7
	312 blocks in segment.6
	312 blocks in segment.5
	312 blocks in segment.4
	312 blocks in segment.3
	312 blocks in segment.2
	312 blocks in segment.1
	312 blocks in segment.0
```

With compaction - Slightly slower writes, faster reads and reduced overall db size (dependent on updates being performed)
```
poetry run python simple_bench.py
4735.32 writes/sec (300000 total in 63.35 sec)
avg  : 0.00021117875496546427
min  : 4.410743713378906e-05
max  : 0.11522579193115234
median: 0.00020623207092285156
stddev: 0.0007418642744548568

13255.71 reads/sec (200000 total in 15.09 sec)
avg  : 7.543921113014221e-05
min  : 0.0
max  : 0.01760411262512207
median: 7.510185241699219e-05
stddev: 6.737602495779222e-05

2.83MB tree size
21.73MB db size
1 segments
	4593 blocks in segment.20
```
