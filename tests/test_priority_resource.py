"""Tests for PriorityResource and PreemptiveResource."""

import netsim


class TestPriorityOrdering:
    def test_priority_ordering_of_requests(self):
        env = netsim.Environment()
        res = netsim.PriorityResource(env, capacity=1)
        log = []

        def user(env, name, priority, hold_time):
            req = res.request(priority=priority)
            yield req
            log.append((name, 'acquired', env.now))
            yield env.timeout(hold_time)
            res.release(req)
            log.append((name, 'released', env.now))

        env.process(user(env, 'A', 0, 5))  # Gets resource immediately
        env.process(user(env, 'B', 10, 1))  # Low priority, waits
        env.process(user(env, 'C', 1, 1))  # High priority, waits but served first

        env.run()
        # A gets it at 0, then C (priority=1) before B (priority=10)
        assert log == [
            ('A', 'acquired', 0),
            ('A', 'released', 5),
            ('C', 'acquired', 5),
            ('C', 'released', 6),
            ('B', 'acquired', 6),
            ('B', 'released', 7),
        ]


class TestPreemption:
    def test_preemption_interrupts_lower_priority(self):
        env = netsim.Environment()
        res = netsim.PreemptiveResource(env, capacity=1)
        log = []

        def user_low(env):
            req = res.request(priority=10)
            try:
                yield req
                log.append(('low', 'acquired', env.now))
                yield env.timeout(100)
                log.append(('low', 'finished', env.now))
            except netsim.Interrupt:
                log.append(('low', 'preempted', env.now))
            finally:
                res.release(req)

        def user_high(env):
            # Wait a bit so low-priority user actually acquires first
            yield env.timeout(1)
            req = res.request(priority=0)
            try:
                yield req
                log.append(('high', 'acquired', env.now))
                yield env.timeout(3)
                log.append(('high', 'finished', env.now))
            except netsim.Interrupt:
                pass
            finally:
                res.release(req)

        env.process(user_low(env))
        env.process(user_high(env))

        env.run()
        assert ('low', 'acquired', 0) in log
        assert ('low', 'preempted', 1) in log
        assert ('high', 'acquired', 1) in log
        assert ('high', 'finished', 4) in log


class TestPreemptedCause:
    def test_preempted_cause_attributes(self):
        env = netsim.Environment()
        res = netsim.PreemptiveResource(env, capacity=1)
        causes = []

        def user_low(env):
            req = res.request(priority=10)
            try:
                yield req
                yield env.timeout(100)
            except netsim.Interrupt as e:
                causes.append(e.cause)
            finally:
                res.release(req)

        def user_high(env):
            req = res.request(priority=0)
            yield req
            yield env.timeout(1)
            res.release(req)

        env.process(user_low(env))
        env.process(user_high(env))
        env.run()

        assert len(causes) == 1
        preempted = causes[0]
        assert isinstance(preempted, netsim.Preempted)
        assert preempted.resource is res
        assert preempted.usage_since == 0
        assert preempted.by is not None  # The high-priority process


class TestPreemptiveResourceNoPreemptFlag:
    def test_preemptive_resource_no_preempt_flag(self):
        """Request with priority=0, preempt=False arrives while a priority=10
        user holds the resource. The priority=10 user should NOT be interrupted."""
        env = netsim.Environment()
        res = netsim.PreemptiveResource(env, capacity=1)
        log = []

        def user_low(env):
            req = res.request(priority=10)
            try:
                yield req
                log.append(('low', 'acquired', env.now))
                yield env.timeout(5)
                log.append(('low', 'finished', env.now))
            except netsim.Interrupt:
                log.append(('low', 'preempted', env.now))
            finally:
                res.release(req)

        def user_high_no_preempt(env):
            yield env.timeout(1)
            req = res.request(priority=0, preempt=False)
            try:
                yield req
                log.append(('high_no_preempt', 'acquired', env.now))
                yield env.timeout(2)
                log.append(('high_no_preempt', 'finished', env.now))
            except netsim.Interrupt:
                log.append(('high_no_preempt', 'preempted', env.now))
            finally:
                res.release(req)

        env.process(user_low(env))
        env.process(user_high_no_preempt(env))
        env.run()

        # Low-priority user should NOT be preempted because preempt=False
        assert ('low', 'acquired', 0) in log
        assert ('low', 'finished', 5) in log
        assert ('low', 'preempted', 1) not in log
        # High-priority (no preempt) user gets the resource after low finishes
        assert ('high_no_preempt', 'acquired', 5) in log
