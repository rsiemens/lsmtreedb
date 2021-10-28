import json
import os
import shutil
import statistics
import sys
import time

from lsmtree.compaction import run_compactor, stop_compactor
from lsmtree.memtable import MemTable
from lsmtree.segment import Segment


def dir_size(dir_path):
    size = 0
    for path, _, files in os.walk(dir_path):
        for file in files:
            full_path = os.path.join(path, file)
            size += os.path.getsize(full_path)
    return size


def sparse_index_len(memtable):
    i = 0
    idx = memtable.sparse_index
    while idx is not None:
        i += 1
        idx = idx.next
    return i


def gather_events(fpath):
    events = []

    with open(fpath, "r") as f:
        for line in f.readlines():
            record = json.loads(line.strip())
            events.append((record["id"], line.strip()))

    return events


def print_stats(stats):
    for k, v in stats.items():
        print(f"{k:<5}: {v}")


def print_results(writes, reads):
    write_stats = {
        "avg": statistics.mean(writes),
        "min": min(writes),
        "max": max(writes),
        "median": statistics.median(writes),
        "stddev": statistics.pstdev(writes),
    }
    read_stats = {
        "avg": statistics.mean(reads),
        "min": min(reads),
        "max": max(reads),
        "median": statistics.median(reads),
        "stddev": statistics.pstdev(reads),
    }

    print(
        f"{len(writes)/sum(writes):.2f} writes/sec ({len(writes)} total in {sum(writes):.2f} sec)"
    )
    print_stats(write_stats)
    print()
    print(
        f"{len(reads)/sum(reads):.2f} reads/sec ({len(reads)} total in {sum(reads):.2f} sec)"
    )
    print_stats(read_stats)


if __name__ == "__main__":
    compaction = True
    if len(sys.argv) > 1 and sys.argv[1] == "--no-compaction":
        compaction = False

    os.mkdir("db")

    write_events = gather_events("example_transactions.jl")
    expected_records = gather_events("expected_state.jl")
    memtable = MemTable(db_dir="db")
    if compaction:
        run_compactor(memtable)

    write_times = []
    for i, write in enumerate(write_events):
        record_id, record = write
        start = time.time()
        memtable[record_id.encode("utf8")] = record.encode("utf8")
        write_times.append(time.time() - start)

    read_times = []
    for i, expected in enumerate(expected_records):
        record_id, record = expected
        start = time.time()
        result = memtable[record_id.encode("utf8")]
        assert result == record.encode("utf8"), f"expected {record.encode('utf8')} got {result}"
        read_times.append(time.time() - start)

    if compaction:
        stop_compactor()

    print_results(write_times, read_times)
    print()
    print(f"{memtable.current_size_bytes / 1048576:.2f}MB tree size")
    print(f"{dir_size('db') / 1048576:.2f}MB db size")
    print(f"{sparse_index_len(memtable)} segments")

    sparse_index = memtable.sparse_index
    while sparse_index:
        with Segment(id=sparse_index.segment, db_dir="db") as segment:
            blocks = sum([1 for _ in segment])
        print(f"\t{blocks} blocks in segment.{sparse_index.segment}")
        sparse_index = sparse_index.next

    shutil.rmtree("db")
