import argparse
import json
import os
import shutil
import statistics
import time

from lsmtree.compaction import run_compactor, stop_compactor
from lsmtree.memtable import MemTable
from lsmtree.segment import Segment


def parse_args():
    parser = argparse.ArgumentParser(description="Run a simple benchmark")
    parser.add_argument("--no-compaction", default=False, action="store_true")
    return parser.parse_args()


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


def report_stats(report, stats):
    for k, v in stats.items():
        report.append(f"{k:<5}: {v}")


def report_results(report, writes, reads):
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

    report.append(
        f"{len(writes)/sum(writes):.2f} writes/sec ({len(writes)} total in {sum(writes):.2f} sec)"
    )
    report_stats(report, write_stats)
    report.append("")
    report.append(
        f"{len(reads)/sum(reads):.2f} reads/sec ({len(reads)} total in {sum(reads):.2f} sec)"
    )
    report_stats(report, read_stats)


if __name__ == "__main__":
    args = parse_args()
    report = []

    os.mkdir("db")

    write_events = gather_events("example_transactions.jl")
    expected_records = gather_events("expected_state.jl")
    memtable = MemTable(db_dir="db")
    if not args.no_compaction:
        run_compactor(memtable)

    print("=========Starting Benchmark=========\n")
    print("- Test write performance [ ]", end="\r", flush=True)
    write_times = []
    for i, write in enumerate(write_events):
        record_id, record = write
        start = time.time()
        memtable[record_id.encode("utf8")] = record.encode("utf8")
        write_times.append(time.time() - start)
    print("- Test write performance [x]")

    print("- Test read performance [ ]", end="\r", flush=True)
    read_times = []
    for i, expected in enumerate(expected_records):
        record_id, record = expected
        start = time.time()
        result = memtable[record_id.encode("utf8")]
        assert result == record.encode(
            "utf8"
        ), f"expected {record.encode('utf8')} got {result}"
        read_times.append(time.time() - start)
    print("- Test read performance [x]")

    if not args.no_compaction:
        stop_compactor()

    report_results(report, write_times, read_times)
    report.append("")
    report.append(f"{memtable.current_size_bytes / 1048576:.2f}MB tree size")
    report.append(f"{dir_size('db') / 1048576:.2f}MB DB size")
    report.append(f"{sparse_index_len(memtable)} segments")

    sparse_index = memtable.sparse_index
    while sparse_index:
        with Segment(id=sparse_index.segment, db_dir="db") as segment:
            blocks = sum([1 for _ in segment])
        report.append(f"\t{blocks} blocks in segment.{sparse_index.segment}")
        sparse_index = sparse_index.next

    # stop the db and start up to recover the RBTree from the WAL
    del memtable

    print("- Test recover from shutdown [ ]", end="\r", flush=True)
    start = time.time()
    restored_memtable = MemTable.reconstruct(db_dir="db")
    print("- Test recover from shutdown [x]")
    report.append("")
    report.append(f"Restored DB from shutdown in {time.time() - start:.2f} sec")

    # validate the data is still all available
    print("- Test validate data integrity from shutdown [ ]", end="\r", flush=True)
    for i, expected in enumerate(expected_records):
        record_id, record = expected
        start = time.time()
        result = restored_memtable[record_id.encode("utf8")]
        assert result == record.encode(
            "utf8"
        ), f"expected {record.encode('utf8')} got {result}"
        read_times.append(time.time() - start)
    print("- Test validate data integrity from shutdown [x]")
    print()
    print("\n".join(report))
    shutil.rmtree("db")
