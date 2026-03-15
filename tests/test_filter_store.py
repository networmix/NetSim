"""Tests for FilterStore resource."""

import netsim


class TestFilteredGet:
    def test_filter_matches(self):
        env = netsim.Environment()
        store = netsim.FilterStore(env)
        results = []

        def producer(env):
            yield store.put({'type': 'apple', 'weight': 1})
            yield store.put({'type': 'banana', 'weight': 2})
            yield store.put({'type': 'apple', 'weight': 3})

        def consumer(env):
            yield env.timeout(1)
            item = yield store.get(filter=lambda x: x['type'] == 'banana')
            results.append(item)

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == [{'type': 'banana', 'weight': 2}]


class TestFilterNoMatchKeepsWaiting:
    def test_no_match_waits(self):
        env = netsim.Environment()
        store = netsim.FilterStore(env)
        results = []

        def producer(env):
            yield store.put('apple')
            yield env.timeout(5)
            yield store.put('banana')

        def consumer(env):
            item = yield store.get(filter=lambda x: x == 'banana')
            results.append((item, env.now))

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        assert results == [('banana', 5)]


class TestFilterStoreOutOfOrder:
    def test_out_of_order_service(self):
        """A later get request whose filter matches should be served before
        an earlier request that doesn't match yet."""
        env = netsim.Environment()
        store = netsim.FilterStore(env)
        log = []

        def producer(env):
            yield store.put('red')
            yield env.timeout(5)
            yield store.put('blue')

        def consumer_blue(env):
            item = yield store.get(filter=lambda x: x == 'blue')
            log.append(('blue_consumer', env.now, item))

        def consumer_red(env):
            item = yield store.get(filter=lambda x: x == 'red')
            log.append(('red_consumer', env.now, item))

        env.process(producer(env))
        # blue consumer registers first but red item arrives first
        env.process(consumer_blue(env))
        env.process(consumer_red(env))
        env.run()
        # Red consumer gets served first (at time 0) even though blue consumer started first
        assert ('red_consumer', 0, 'red') in log
        assert ('blue_consumer', 5, 'blue') in log
