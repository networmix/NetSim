"""Tests for SortedQueue."""

import pytest

from netsim.resources import SortedQueue


class _KeyItem:
    """Helper item with a .key attribute for SortedQueue tests."""

    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return f'_KeyItem({self.key})'


class TestSortedQueueMaintainsOrder:
    def test_sorted_queue_maintains_order(self):
        """Insert items out of order, verify they come out sorted."""
        sq = SortedQueue()
        sq.append(_KeyItem(30))
        sq.append(_KeyItem(10))
        sq.append(_KeyItem(20))
        keys = [item.key for item in sq]
        assert keys == [10, 20, 30]


class TestSortedQueueMaxlenExceededRaises:
    def test_sorted_queue_maxlen_exceeded_raises(self):
        """Create SortedQueue(maxlen=2), add 2 items, assert 3rd raises RuntimeError."""
        sq = SortedQueue(maxlen=2)
        sq.append(_KeyItem(1))
        sq.append(_KeyItem(2))
        with pytest.raises(RuntimeError, match='Queue is full'):
            sq.append(_KeyItem(3))
