"""
Implementation of Robert Sedgewick's Left Leaning Red Black Tree from the book
Algorithms Fourth Edition. https://www.cs.princeton.edu/~rs/talks/LLRB/LLRB.pdf

This implementation doesn't provide support for node deletion since LSMtree
databases are append only. Instead a tombstone value is added for a keyspace
which the file compaction job will cleanup.
"""

RED = True
BLACK = False


class Node:
    def __init__(self, key, value, size=1, color=RED):
        self.key = key
        self.value = value
        self.size = size
        self.color = color
        self.left = None
        self.right = None


class RBTree:
    def __init__(self):
        self.root = None

    def __len__(self):
        if self.root is None:
            return 0
        return self.root.size

    def _size(self, node):
        if node is None:
            return 0
        return node.size

    def _is_red(self, node):
        if node is None:
            return False
        return node.color == RED

    def _rotate_left(self, old_root):
        """
        Rotates the root of a subtree so that it's right child
        is the new root and the old root becomes the left child.
        """
        new_root = old_root.right
        old_root.right = new_root.left
        new_root.left = old_root
        new_root.color = old_root.color
        old_root.color = RED
        new_root.size = old_root.size
        old_root.size = self._size(old_root.left) + self._size(old_root.right) + 1
        return new_root

    def _rotate_right(self, old_root):
        """
        Inverse of `_rotate_left`.
        """
        new_root = old_root.left
        old_root.left = new_root.right
        new_root.right = old_root
        new_root.color = old_root.color
        old_root.color = RED
        new_root.size = old_root.size
        old_root.size = self._size(old_root.left) + self._size(old_root.right) + 1
        return new_root

    def _flip_colors(self, node):
        node.left.color = BLACK
        node.right.color = BLACK
        node.color = RED

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key):
        current = self.root

        while current is not None:
            if key < current.key:
                current = current.left
            elif key > current.key:
                current = current.right
            else:
                return current.value

        raise KeyError(key)

    def _set_recursive(self, node, key, value):
        if node is None:
            return Node(key, value, size=1, color=RED)

        if key < node.key:
            node.left = self._set_recursive(node.left, key, value)
        elif key > node.key:
            node.right = self._set_recursive(node.right, key, value)
        else:
            node.value = value

        if not self._is_red(node.left) and self._is_red(node.right):
            node = self._rotate_left(node)
        if self._is_red(node.left) and self._is_red(node.left.left):
            node = self._rotate_right(node)
        if self._is_red(node.left) and self._is_red(node.right):
            self._flip_colors(node)

        node.size = self._size(node.left) + self._size(node.right) + 1
        return node

    def __setitem__(self, key, value):
        self.root = self._set_recursive(self.root, key, value)
        self.root.color = BLACK

    def _gather_keys(self, queue, node, include_items=False):
        """
        Inorder tree traversal to get the keys in sorted order.
        """
        if node is None:
            return

        if node.left:
            self._gather_keys(queue, node.left, include_items=include_items)

        if include_items:
            queue.append((node.key, node.value))
        else:
            queue.append(node.key)

        if node.right:
            self._gather_keys(queue, node.right, include_items=include_items)

    def __iter__(self):
        keys = []

        self._gather_keys(keys, self.root)
        return iter(keys)

    def items(self):
        items = []
        self._gather_keys(items, self.root, include_items=True)
        return iter(items)
