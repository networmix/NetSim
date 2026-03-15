"""Tests for PriorityStore and PriorityItem."""

import netsim


class TestPriorityStoreOrder:
    def test_items_retrieved_in_priority_order(self):
        env = netsim.Environment()
        store = netsim.PriorityStore(env)
        results = []

        def producer(env):
            yield store.put(30)
            yield store.put(10)
            yield store.put(20)

        def consumer(env):
            yield env.timeout(1)
            for _ in range(3):
                item = yield store.get()
                results.append(item)

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == [10, 20, 30]


class TestPriorityItem:
    def test_priority_item_for_unorderable_items(self):
        """PriorityItem wraps unorderable items so they can be stored."""
        env = netsim.Environment()
        store = netsim.PriorityStore(env)
        results = []

        # Dicts are not orderable, so we wrap them in PriorityItem
        def producer(env):
            yield store.put(netsim.PriorityItem(3, {'name': 'c'}))
            yield store.put(netsim.PriorityItem(1, {'name': 'a'}))
            yield store.put(netsim.PriorityItem(2, {'name': 'b'}))

        def consumer(env):
            yield env.timeout(1)
            for _ in range(3):
                item = yield store.get()
                results.append(item.item['name'])

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == ['a', 'b', 'c']

    def test_priority_item_comparison(self):
        a = netsim.PriorityItem(1, 'x')
        b = netsim.PriorityItem(2, 'y')
        assert a < b
        assert not b < a


class TestPriorityItemAllComparisons:
    def test_priority_item_all_comparisons(self):
        """Test that __lt__, __le__, __gt__, __ge__, __eq__ all compare by
        priority only (not by item)."""
        a = netsim.PriorityItem(1, 'aaa')
        b = netsim.PriorityItem(2, 'bbb')
        c = netsim.PriorityItem(1, 'zzz')  # same priority as a, different item

        # __lt__
        assert a < b
        assert not b < a
        assert not a < c  # same priority

        # __le__
        assert a <= b
        assert a <= c  # same priority
        assert not b <= a

        # __gt__
        assert b > a
        assert not a > b
        assert not a > c  # same priority

        # __ge__
        assert b >= a
        assert a >= c  # same priority
        assert not a >= b

        # __eq__ compares by priority only
        assert a == c  # same priority, different items
        assert not a == b  # different priority

        # __eq__ with non-PriorityItem returns NotImplemented
        assert a.__eq__(42) is NotImplemented
