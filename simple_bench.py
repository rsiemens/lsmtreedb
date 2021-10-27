import json
import os
import shutil
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


if __name__ == "__main__":
    compaction = True
    if len(sys.argv) > 1 and sys.argv[1] == '--no-compaction':
        compaction = False

    os.mkdir("db")

    write_events = gather_events("example_transactions.jl")
    expected_records = gather_events("expected_state.jl")
    memtable = MemTable(db_dir="db")
    if compaction:
        run_compactor(memtable)

    start = time.time()
    for i, write in enumerate(write_events):
        record_id, record = write
        memtable[record_id.encode("utf8")] = record.encode("utf8")
    total = time.time() - start
    print(f"{i / total:.2f} writes/sec ({i} total writes in {total:.2f} sec).")

    start = time.time()
    for i, expected in enumerate(expected_records):
        record_id, record = expected
        assert memtable[record_id.encode("utf8")] == record.encode("utf8")
    total = time.time() - start
    print(f"{i / total:.2f} reads/sec ({i} total reads in {total:.2f} sec).")

    if compaction:
        stop_compactor()
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
