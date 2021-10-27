"""
Performs background compaction on segment files.
"""
import os

from .rbtree import RBTree


class Compactor:
    def __init__(self, db_dir, max_segments_per_run=3):
        self.db_dir = db_dir
        self.max_segments_per_run = max_segments_per_run
        self.rbtree = RBTree()

    def get_target_segments(self):
        segments = []

        for file in os.listdir(self.db_dir):
            if os.path.isfile(os.path.join(self.db_dir, file)):
                segment_id = int(file.split(".")[1])
                segments.append(segment_id)

        return sorted(segments)[: self.max_segments_per_run]

    def compact(self):
        targets = self.get_target_segments()
        file_handles = [open(f"segments.{t}", "rb") for t in targets]
        # iterator over the blocks in a segment file
        # decode k,v pairs in the blocks
        # find the lowest key of the read keys from the blocks
        # if two keys are the same take the one from the highest numbered segment
        # if it's a tombstone continue otherwise add it to the rbtree
        # flush the tree to a temporart `compacted.segment.{highest-read-segment}`
        # acquire locks on the memtable sparse_index belonging the highest segment value
        # delete the old segments and move the compact segment to `segement.{highest-read-segment}`

    def iter_segment_blocks(self, segment):
        pass
