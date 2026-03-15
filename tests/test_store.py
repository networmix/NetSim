"""Tests for Store resource."""

import pytest

import netsim


class TestStorePutAndGet:
    def test_put_and_get_basic(self):
        env = netsim.Environment()
        store = netsim.Store(env)
        results = []

        def producer(env):
            yield store.put('item1')
            yield store.put('item2')

        def consumer(env):
            item = yield store.get()
            results.append(item)
            item = yield store.get()
            results.append(item)

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == ['item1', 'item2']


class TestStoreFIFO:
    def test_fifo_order(self):
        env = netsim.Environment()
        store = netsim.Store(env)
        results = []

        def producer(env):
            yield store.put('first')
            yield store.put('second')
            yield store.put('third')

        def consumer(env):
            yield env.timeout(1)  # Let all items be put first
            for _ in range(3):
                item = yield store.get()
                results.append(item)

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == ['first', 'second', 'third']


class TestStoreBlockingWhenEmpty:
    def test_consumer_waits_for_producer(self):
        env = netsim.Environment()
        store = netsim.Store(env)
        log = []

        def consumer(env):
            log.append(('consumer_wait', env.now))
            item = yield store.get()
            log.append(('consumer_got', env.now, item))

        def producer(env):
            yield env.timeout(5)
            log.append(('producer_put', env.now))
            yield store.put('delayed_item')

        env.process(consumer(env))
        env.process(producer(env))
        env.run()
        assert log == [
            ('consumer_wait', 0),
            ('producer_put', 5),
            ('consumer_got', 5, 'delayed_item'),
        ]


class TestStoreCapacity:
    def test_producer_blocks_when_full(self):
        env = netsim.Environment()
        store = netsim.Store(env, capacity=2)
        log = []

        def producer(env):
            yield store.put('a')
            log.append(('put_a', env.now))
            yield store.put('b')
            log.append(('put_b', env.now))
            yield store.put('c')  # Should block until consumer gets
            log.append(('put_c', env.now))

        def consumer(env):
            yield env.timeout(5)
            item = yield store.get()
            log.append(('got', env.now, item))

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert ('put_a', 0) in log
        assert ('put_b', 0) in log
        assert ('put_c', 5) in log


class TestStoreUnlimitedCapacity:
    def test_unlimited_capacity(self):
        env = netsim.Environment()
        store = netsim.Store(env)  # default is inf
        results = []

        def producer(env):
            for i in range(100):
                yield store.put(i)

        def consumer(env):
            yield env.timeout(1)
            for _ in range(100):
                item = yield store.get()
                results.append(item)

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == list(range(100))


class TestStorePutFireAndForget:
    def test_store_put_fire_and_forget(self):
        """Call store.put(item) without yielding. Then yield store.get().
        Verify the item is retrieved."""
        env = netsim.Environment()
        store = netsim.Store(env)
        results = []

        def proc(env):
            store.put('fire_and_forget')  # no yield
            item = yield store.get()
            results.append(item)

        env.process(proc(env))
        env.run()
        assert results == ['fire_and_forget']


class TestStoreZeroCapacityRaises:
    def test_store_zero_capacity_raises(self):
        """Store(env, capacity=0) raises ValueError."""
        env = netsim.Environment()
        with pytest.raises(ValueError):
            netsim.Store(env, capacity=0)


class TestStorePutCancel:
    def test_store_put_cancel(self):
        """Put into a full store (capacity=1, already has item). Cancel the
        pending put. Verify put_queue is empty."""
        env = netsim.Environment()
        store = netsim.Store(env, capacity=1)
        results = []

        def proc(env):
            yield store.put('first')  # fills the store
            put_evt = store.put('second')  # should be pending (store full)
            assert not put_evt.triggered
            put_evt.cancel()
            assert len(store.put_queue) == 0
            results.append('done')

        env.process(proc(env))
        env.run()
        assert results == ['done']


class TestStoreGetCancel:
    def test_store_get_cancel(self):
        """Get from an empty store. Cancel the pending get. Verify get_queue is empty."""
        env = netsim.Environment()
        store = netsim.Store(env)
        results = []

        def proc(env):
            get_evt = store.get()  # store is empty, should be pending
            assert not get_evt.triggered
            get_evt.cancel()
            assert len(store.get_queue) == 0
            results.append('done')
            yield env.timeout(0)

        env.process(proc(env))
        env.run()
        assert results == ['done']
