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

Example
```
poetry run python generate_example_dataset.py 200000

poetry run python simple_bench.py --no-compaction
58161.83 writes/sec (299999 total writes in 5.16 sec).
1464.54 reads/sec (199999 total reads in 136.56 sec).
2.83MB tree size
26.69MB db size
21 segments
	311 blocks in segment.20
	311 blocks in segment.19
	311 blocks in segment.18
	311 blocks in segment.17
	311 blocks in segment.16
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
	312 blocks in segment.5
	312 blocks in segment.4
	312 blocks in segment.3
	312 blocks in segment.2
	312 blocks in segment.1
	312 blocks in segment.0

poetry run python simple_bench.py
55401.17 writes/sec (299999 total writes in 5.42 sec).
6010.41 reads/sec (199999 total reads in 33.28 sec).
2.83MB tree size
18.66MB db size
1 segments
	4593 blocks in segment.20
```
