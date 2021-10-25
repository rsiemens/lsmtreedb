import random
from copy import copy
from string import ascii_letters

import pytest

from lsmtree.rbtree import BLACK, RED, Node, RBTree


def test_len():
    tree = RBTree()
    assert len(tree) == 0

    for i, key in enumerate(ascii_letters, start=1):
        tree[key] = key
        assert len(tree) == i


def test_is_red():
    tree = RBTree()
    black_node = Node(key=1, value=2, color=BLACK)
    red_node = Node(key=3, value=4, color=RED)

    assert not tree._is_red(None)
    assert not tree._is_red(black_node)
    assert tree._is_red(red_node)


def test_get_and_set_item():
    tree = RBTree()

    with pytest.raises(KeyError):
        tree["foo"]

    tree["foo"] = "bar"
    assert tree["foo"] == "bar"

    tree["foo"] = "baz"
    assert tree["foo"] == "baz"


def test_iter():
    tree = RBTree()

    for k in [5, 2, 3, 1, 4]:
        tree[k] = k

    assert [k for k in tree] == [1, 2, 3, 4, 5]
