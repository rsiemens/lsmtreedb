"""
Performs background compaction on segment files.
"""
import os
import threading
import time

from .memtable import TOMBSTONE, SparseIndex
from .segment import Block, Segment
from .settings import BLOCK_COMPRESSION, BLOCK_SIZE

_RUNNING = False
_STOP_EVENT = threading.Event()


def run_compactor(memtable, interval=1):
    if _RUNNING:
        return
    compactor = Compactor(memtable, interval)
    thread = threading.Thread(target=compactor.run, daemon=True)
    thread.start()


def stop_compactor():
    _STOP_EVENT.set()
    while _RUNNING:
        pass


class Compactor:
    def __init__(self, memtable, interval=1):
        self.memtable = memtable
        self.db_dir = memtable.db_dir
        self.interval = interval

    def get_target_segments(self):
        segments = []

        for file in os.listdir(self.db_dir):
            if os.path.isfile(os.path.join(self.db_dir, file)):
                segment_id = int(file.split(".")[1])
                segments.append(segment_id)

        return sorted(segments)[:2]

    def compact(self):
        """
        Works as follows:
         - find the oldest two segment files
         - iteratively compact them by taking advantage of the fact that key
           values are in sorted order within the segment files.
         - acquire a lock on the sparse_index
         - remove the old files and replace with the new compacted one
         - unlink the old sparse_index node and update it with the new one
         - release the lock
        """
        targets = self.get_target_segments()
        if len(targets) != 2:
            return

        with Segment(
            id=max(targets), db_dir=self.db_dir, fname="_compact_segment"
        ) as segment:
            index = SparseIndex(entries=[], segment=segment.id)
            block = Block()

            for key, val in self.iter_smallest_kv_pair(*targets):
                if val == TOMBSTONE:
                    continue

                block.add(key, val)

                if len(block) > BLOCK_SIZE:
                    bytes_written = segment.write(
                        block.dump(compress=BLOCK_COMPRESSION)
                    )
                    eof_pos = segment.tell_eof
                    index.add(block.key, (eof_pos - bytes_written, eof_pos))
                    block = Block()

            bytes_written = segment.write(block.dump(compress=BLOCK_COMPRESSION))
            index.add(block.key, (segment.tell_eof - bytes_written, segment.tell_eof))

        with self.memtable.sparse_index_lock:
            for i in targets:
                os.remove(os.path.join(self.db_dir, f"segment.{i}"))

            os.rename(
                os.path.join(self.db_dir, f"_compact_segment.{max(targets)}"),
                os.path.join(self.db_dir, f"segment.{max(targets)}"),
            )

            previous_index = None
            sparse_index = self.memtable.sparse_index
            while sparse_index:
                if sparse_index.segment == index.segment:
                    if previous_index is None:
                        self.memtable.sparse_index = index
                    else:
                        previous_index.next = index
                    break
                previous_index = sparse_index
                sparse_index = sparse_index.next

    def iter_kv_pairs(self, target):
        with Segment(id=target, db_dir=self.db_dir) as segment:
            for _, raw_block in segment:
                for k, v in Block.iter_from_binary(raw_block):
                    yield k, v

    def iter_smallest_kv_pair(self, segment_a, segment_b):
        default = (None, None)
        segment_a_generator = self.iter_kv_pairs(segment_a)
        segment_b_generator = self.iter_kv_pairs(segment_b)
        segment_a_key, segment_a_val = next(segment_a_generator, default)
        segment_b_key, segment_b_val = next(segment_b_generator, default)

        while segment_a_key is not None or segment_b_key is not None:
            if segment_a_key is None:
                yield segment_b_key, segment_b_val
                segment_b_key, segment_b_val = next(segment_b_generator, default)
            elif segment_b_key is None:
                yield segment_a_key, segment_a_val
                segment_a_key, segment_a_val = next(segment_a_generator, default)
            elif segment_a_key < segment_b_key:
                yield segment_a_key, segment_a_val
                segment_a_key, segment_a_val = next(segment_a_generator, default)
            elif segment_b_key < segment_a_key:
                yield segment_b_key, segment_b_val
                segment_b_key, segment_b_val = next(segment_b_generator, default)
            # keys are the same so the newer segment wins and the old one is discarded
            else:
                yield segment_b_key, segment_b_val
                segment_a_key, segment_a_val = next(segment_a_generator, default)
                segment_b_key, segment_b_val = next(segment_b_generator, default)

    def run(self):
        _RUNNING = True
        try:
            while not _STOP_EVENT.is_set():
                start = time.time()
                self.compact()
                total = time.time() - start

                if total < self.interval:
                    time.sleep(self.interval - total)
        finally:
            _RUNNING = False
