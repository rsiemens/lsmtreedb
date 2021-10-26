import os
import shutil
import time

from lsmtree.memtable import MemTable

def dir_size(dir_path):
    size = 0
    for path, _, files in os.walk(dir_path):
        for file in files:
            full_path = os.path.join(path, file)
            size += os.path.getsize(full_path)
    return size


if __name__ == "__main__":
    os.mkdir("db")
    memtable = MemTable(base_dir="db")

    with open("/usr/share/dict/words", "r") as file:
        start = time.time()
        for lineno, line in enumerate(file.readlines()):
            memtable[line.strip().encode("utf8")] = b"%d" % lineno
        total = time.time() - start
        print(f"{lineno / total:.2f} writes/sec ({lineno} total writes in {total:.2f} sec).")

    with open("/usr/share/dict/words", "r") as file:
        start = time.time()
        for lineno, line in enumerate(file.readlines()):
            assert memtable[line.strip().encode("utf8")] == b"%d" % lineno
        total = time.time() - start
        print(f"{lineno / total:.2f} reads/sec ({lineno} total reads in {total:.2f} sec).")

    print(f"{memtable.current_size_bytes / 1024:.2f}KB tree size")
    print(f"{dir_size('db') / 1024:.2f}KB db size")
    print(f"{memtable.sparse_index_len} segments")
    shutil.rmtree("db")
