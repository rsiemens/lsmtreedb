import os
import shutil
import time

from lsmtree.memtable import MemTable

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

    shutil.rmtree("db")
