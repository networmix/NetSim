"""Tests for Container resource."""

import pytest

import netsim


class TestContainerPutAndGet:
    def test_put_and_get_amounts(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=100, init=0)

        def proc(env):
            yield tank.put(30)
            assert tank.level == 30
            yield tank.get(10)
            assert tank.level == 20

        env.process(proc(env))
        env.run()


class TestContainerLevelTracking:
    def test_level_tracks_correctly(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=50, init=20)
        assert tank.level == 20

        def proc(env):
            yield tank.put(10)
            assert tank.level == 30
            yield tank.get(25)
            assert tank.level == 5

        env.process(proc(env))
        env.run()


class TestContainerCapacityLimits:
    def test_capacity_property(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=50)
        assert tank.capacity == 50


class TestContainerBlockingWhenFull:
    def test_blocks_when_full(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=10, init=8)
        log = []

        def producer(env):
            yield tank.put(5)  # Needs 5 but only 2 free -> blocks
            log.append(('put_done', env.now))

        def consumer(env):
            yield env.timeout(3)
            yield tank.get(5)
            log.append(('got', env.now))

        env.process(producer(env))
        env.process(consumer(env))
        env.run()
        # Consumer gets 5 at time 3 (level goes from 8 to 3, freeing 7).
        # Then producer's put of 5 can proceed (level 3+5=8 <= 10).
        assert ('got', 3) in log
        assert ('put_done', 3) in log


class TestContainerBlockingWhenEmpty:
    def test_blocks_when_empty(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=100, init=0)
        log = []

        def consumer(env):
            yield tank.get(10)  # Nothing to get -> blocks
            log.append(('got', env.now))

        def producer(env):
            yield env.timeout(5)
            yield tank.put(20)
            log.append(('put', env.now))

        env.process(consumer(env))
        env.process(producer(env))
        env.run()
        assert ('put', 5) in log
        assert ('got', 5) in log


class TestContainerInitLevel:
    def test_init_level(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=100, init=42)
        assert tank.level == 42


class TestContainerValidation:
    def test_capacity_zero_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='capacity'):
            netsim.Container(env, capacity=0)

    def test_capacity_negative_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='capacity'):
            netsim.Container(env, capacity=-5)

    def test_init_negative_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='init'):
            netsim.Container(env, capacity=10, init=-1)

    def test_init_exceeds_capacity_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='init'):
            netsim.Container(env, capacity=10, init=20)

    def test_amount_zero_put_raises(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=10)
        with pytest.raises(ValueError, match='amount'):
            tank.put(0)

    def test_amount_negative_put_raises(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=10)
        with pytest.raises(ValueError, match='amount'):
            tank.put(-1)

    def test_amount_zero_get_raises(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=10, init=5)
        with pytest.raises(ValueError, match='amount'):
            tank.get(0)

    def test_amount_negative_get_raises(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=10, init=5)
        with pytest.raises(ValueError, match='amount'):
            tank.get(-1)
