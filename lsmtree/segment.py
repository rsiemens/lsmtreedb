import io
import os


class Segment:
    """
    Represents a file segment which contains the keys and values of a tree in
    sorted order.

    Reads and writes a segment file:
    """

    def __init__(self, id, base_dir):
        self.id = id
        self.path = os.path.join(base_dir, f"segment.{self.id}")
        self.file = None

    def open(self):
        if not os.path.exists(self.path):
            self.file = open(self.path, "w+b")
        else:
            self.file = open(self.path, "r+b")

    def close(self):
        self.file.close()

    def read_range(self, start, end=-1):
        self.file.seek(start)

        if end == -1:
            return self.file.read()
        return self.file.read(end - start)

    def write(self, chunk):
        self.file.seek(0, io.SEEK_END)
        self.file.write(chunk)
        self.file.flush()
        os.fsync(self.file.fileno())

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
