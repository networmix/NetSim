"""Integration tests: multi-process interactions and complex scenarios."""

import netsim


class TestProducerConsumerThroughStore:
    def test_multi_process_producer_consumer(self):
        env = netsim.Environment()
        store = netsim.Store(env)
        produced = []
        consumed = []

        def producer(env, name, items):
            for item in items:
                yield env.timeout(1)
                yield store.put(f'{name}:{item}')
                produced.append(f'{name}:{item}')

        def consumer(env, name, count):
            for _ in range(count):
                item = yield store.get()
                consumed.append((name, item))
                yield env.timeout(2)

        env.process(producer(env, 'P1', [1, 2, 3]))
        env.process(producer(env, 'P2', [4, 5, 6]))
        env.process(consumer(env, 'C1', 3))
        env.process(consumer(env, 'C2', 3))
        env.run()

        assert len(produced) == 6
        assert len(consumed) == 6
        # All produced items should be consumed
        produced_set = set(produced)
        consumed_items = {item for _, item in consumed}
        assert produced_set == consumed_items


class TestComplexSynchronization:
    def test_allof_anyof_sync(self):
        env = netsim.Environment()
        log = []

        def task(env, name, duration):
            yield env.timeout(duration)
            log.append((name, env.now))
            return name

        def coordinator(env):
            t1 = env.process(task(env, 'fast', 2))
            t2 = env.process(task(env, 'medium', 5))
            t3 = env.process(task(env, 'slow', 10))

            # Wait for any one to finish
            yield t1 | t2 | t3
            log.append(('any_done', env.now))

            # Wait for all to finish
            yield t1 & t2 & t3
            log.append(('all_done', env.now))

        env.process(coordinator(env))
        env.run()

        assert ('any_done', 2) in log
        assert ('all_done', 10) in log


class TestInterruptDuringResourceWait:
    def test_interrupt_while_waiting_for_resource(self):
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=1)
        log = []

        def holder(env):
            req = res.request()
            yield req
            log.append(('holder_acquired', env.now))
            yield env.timeout(100)
            res.release(req)

        def waiter(env):
            req = res.request()
            try:
                yield req
                log.append(('waiter_acquired', env.now))
            except netsim.Interrupt as e:
                log.append(('waiter_interrupted', env.now, e.cause))
                req.cancel()

        def interrupter(env, target):
            yield env.timeout(5)
            target.interrupt('abort')

        env.process(holder(env))
        w = env.process(waiter(env))
        env.process(interrupter(env, w))
        env.run(until=20)

        assert ('holder_acquired', 0) in log
        assert ('waiter_interrupted', 5, 'abort') in log
        assert ('waiter_acquired', 5) not in log


class TestProcessWaitingForProcess:
    def test_yield_process_gets_return_value(self):
        env = netsim.Environment()
        results = []

        def child1(env):
            yield env.timeout(3)
            return 'child1_done'

        def child2(env):
            yield env.timeout(5)
            return 'child2_done'

        def parent(env):
            p1 = env.process(child1(env))
            p2 = env.process(child2(env))

            r1 = yield p1
            results.append((r1, env.now))

            r2 = yield p2
            results.append((r2, env.now))

        env.process(parent(env))
        env.run()

        assert results == [('child1_done', 3), ('child2_done', 5)]


class TestPipelineProcessing:
    def test_multi_stage_pipeline(self):
        env = netsim.Environment()
        stage1_out = netsim.Store(env)
        stage2_out = netsim.Store(env)
        final_results = []

        def stage1(env):
            for i in range(5):
                yield env.timeout(1)
                yield stage1_out.put(i * 10)

        def stage2(env):
            for _ in range(5):
                item = yield stage1_out.get()
                yield env.timeout(2)
                yield stage2_out.put(item + 1)

        def collector(env):
            for _ in range(5):
                item = yield stage2_out.get()
                final_results.append(item)

        env.process(stage1(env))
        env.process(stage2(env))
        env.process(collector(env))
        env.run()

        assert final_results == [1, 11, 21, 31, 41]


class TestResourcePreemptionChain:
    def test_preemption_chain(self):
        env = netsim.Environment()
        res = netsim.PreemptiveResource(env, capacity=1)
        log = []

        def user(env, name, priority, duration):
            req = res.request(priority=priority)
            try:
                yield req
                log.append((name, 'start', env.now))
                yield env.timeout(duration)
                log.append((name, 'finish', env.now))
            except netsim.Interrupt:
                log.append((name, 'preempted', env.now))
            finally:
                res.release(req)

        # Low priority starts first
        env.process(user(env, 'low', 10, 100))
        # Medium arrives, preempts low
        env.process(user(env, 'med', 5, 50))
        # High arrives, preempts medium
        env.process(user(env, 'high', 1, 5))

        env.run()

        assert ('low', 'preempted', 0) in log
        assert ('med', 'preempted', 0) in log
        assert ('high', 'start', 0) in log
        assert ('high', 'finish', 5) in log
