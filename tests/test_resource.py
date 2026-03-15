"""Tests for Resource (mutex-like)."""

import pytest

import netsim


class TestResourceRequestRelease:
    def test_request_and_release(self):
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=1)
        log = []

        def user(env, name):
            req = res.request()
            yield req
            log.append((name, 'acquired', env.now))
            yield env.timeout(2)
            res.release(req)
            log.append((name, 'released', env.now))

        env.process(user(env, 'A'))
        env.run()
        assert log == [('A', 'acquired', 0), ('A', 'released', 2)]


class TestResourceQueuing:
    def test_queuing_when_at_capacity(self):
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=1)
        log = []

        def user(env, name, hold_time):
            req = res.request()
            yield req
            log.append((name, 'acquired', env.now))
            yield env.timeout(hold_time)
            res.release(req)
            log.append((name, 'released', env.now))

        env.process(user(env, 'A', 3))
        env.process(user(env, 'B', 2))
        env.run()
        assert log == [
            ('A', 'acquired', 0),
            ('A', 'released', 3),
            ('B', 'acquired', 3),
            ('B', 'released', 5),
        ]


class TestResourceMultipleUsers:
    def test_multiple_users_up_to_capacity(self):
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=2)
        log = []

        def user(env, name, hold_time):
            req = res.request()
            yield req
            log.append((name, 'acquired', env.now))
            yield env.timeout(hold_time)
            res.release(req)
            log.append((name, 'released', env.now))

        env.process(user(env, 'A', 5))
        env.process(user(env, 'B', 3))
        env.process(user(env, 'C', 2))
        env.run()
        # A and B acquire at time 0 (capacity=2), C waits
        assert ('A', 'acquired', 0) in log
        assert ('B', 'acquired', 0) in log
        # C acquires after B releases at time 3
        assert ('C', 'acquired', 3) in log


class TestResourceContextManager:
    def test_context_manager_auto_release(self):
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=1)
        log = []

        def user(env, name, hold_time):
            with res.request() as req:
                yield req
                log.append((name, 'acquired', env.now))
                yield env.timeout(hold_time)
            log.append((name, 'released', env.now))

        env.process(user(env, 'A', 2))
        env.process(user(env, 'B', 1))
        env.run()
        assert ('A', 'acquired', 0) in log
        assert ('B', 'acquired', 2) in log


class TestResourceCount:
    def test_count_property(self):
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=2)
        counts = []

        def proc(env):
            counts.append(res.count)  # 0
            req1 = res.request()
            yield req1
            counts.append(res.count)  # 1
            req2 = res.request()
            yield req2
            counts.append(res.count)  # 2
            res.release(req1)
            yield env.timeout(0)  # let release process
            counts.append(res.count)  # 1

        env.process(proc(env))
        env.run()
        assert counts == [0, 1, 2, 1]


class TestResourceReleaseUnknownRequest:
    def test_resource_release_unknown_request(self):
        """Create a Request but don't yield it. Call resource.release(request).
        Should succeed silently (no error)."""
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=1)

        def proc(env):
            # Create a request without yielding it
            from netsim.resources import Request

            req = Request(res)
            # Release it without ever acquiring -- should not error
            res.release(req)
            yield env.timeout(0)

        env.process(proc(env))
        env.run()  # should complete without error


class TestResourceZeroCapacityRaises:
    def test_resource_zero_capacity_raises(self):
        """Resource(env, capacity=0) raises ValueError."""
        env = netsim.Environment()
        with pytest.raises(ValueError):
            netsim.Resource(env, capacity=0)
