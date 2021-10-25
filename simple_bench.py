import os
import shutil
import time

from lsmtree.memtable import MemTable

if __name__ == "__main__":
    try:
        shutil.rmtree("db")
    except OSError:
        pass

    os.mkdir("db")
    memtable = MemTable(base_dir="db")

    with open("/usr/share/dict/words", "r") as file:
        start = time.time()
        for lineno, line in enumerate(file.readlines()):
            memtable[line.strip().encode("utf8")] = b"%d" % lineno
        print(f"Wrote {lineno} words in {time.time() - start} sec")

    with open("/usr/share/dict/words", "r") as file:
        start = time.time()
        for lineno, line in enumerate(file.readlines()):
            assert memtable[line.strip().encode("utf8")] == b"%d" % lineno
        print(f"Read {lineno} words in {time.time() - start} sec")
