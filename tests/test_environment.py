"""Tests for Environment: run modes, peek, step, and active_process."""

import pytest

import netsim


class TestRunUntilTime:
    def test_run_until_time(self):
        env = netsim.Environment()

        def proc(env):
            while True:
                yield env.timeout(1)

        env.process(proc(env))
        env.run(until=10)
        assert env.now == 10

    def test_run_until_time_no_overshoot(self):
        env = netsim.Environment()
        log = []

        def proc(env):
            while True:
                yield env.timeout(3)
                log.append(env.now)

        env.process(proc(env))
        env.run(until=10)
        assert all(t <= 10 for t in log)


class TestRunUntilEvent:
    def test_run_until_event(self):
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(5)
            return 'result'

        p = env.process(proc(env))
        val = env.run(until=p)
        assert val == 'result'
        assert env.now == 5

    def test_run_until_already_processed_event(self):
        env = netsim.Environment()
        evt = env.event()
        evt.succeed(value='already')
        env.step()  # process it
        val = env.run(until=evt)
        assert val == 'already'


class TestRunUntilEmpty:
    def test_run_until_no_events(self):
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(5)

        env.process(proc(env))
        env.run()
        assert env.now == 5


class TestPeek:
    def test_peek_returns_next_event_time(self):
        env = netsim.Environment()
        env.timeout(5)
        # Timeout is scheduled at now + delay = 0 + 5 = 5
        assert env.peek() == 5

    def test_peek_infinity_when_empty(self):
        env = netsim.Environment()
        assert env.peek() == netsim.Infinity


class TestStep:
    def test_step_advances_time(self):
        env = netsim.Environment()
        env.timeout(3)
        env.step()
        assert env.now == 3

    def test_empty_schedule_raises(self):
        env = netsim.Environment()
        with pytest.raises(netsim.EmptySchedule):
            env.step()


class TestRunUntilInvalid:
    def test_until_le_now_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='must be >'):
            env.run(until=0)

    def test_until_less_than_now_raises(self):
        env = netsim.Environment()
        env.timeout(5)
        env.step()  # now == 5
        with pytest.raises(ValueError, match='must be >'):
            env.run(until=3)

    def test_until_equal_to_now_raises(self):
        env = netsim.Environment()
        env.timeout(5)
        env.step()
        with pytest.raises(ValueError, match='must be >'):
            env.run(until=5)


class TestActiveProcess:
    def test_active_process_during_execution(self):
        env = netsim.Environment()
        active_procs = []

        def proc(env):
            active_procs.append(env.active_process)
            yield env.timeout(1)
            active_procs.append(env.active_process)

        p = env.process(proc(env))
        env.run()
        assert active_procs[0] is p
        assert active_procs[1] is p

    def test_active_process_none_outside(self):
        env = netsim.Environment()
        assert env.active_process is None


class TestInitialTime:
    def test_initial_time(self):
        env = netsim.Environment(initial_time=100)
        assert env.now == 100

        def proc(env):
            yield env.timeout(5)

        env.process(proc(env))
        env.run()
        assert env.now == 105


class TestRunUntilUntriggeredEventRaises:
    def test_run_until_untriggered_event_raises_runtime_error(self):
        """Create a pending event (never triggered). Run env.run(until=event).
        Assert RuntimeError with 'No scheduled events left'."""
        env = netsim.Environment()
        pending = env.event()  # never triggered
        with pytest.raises(RuntimeError, match='No scheduled events left'):
            env.run(until=pending)


class TestRunUntilFailedEventRaises:
    def test_run_until_failed_event_raises(self):
        """Create a process that raises ValueError. Run env.run(until=process).
        Assert ValueError propagates."""
        env = netsim.Environment()

        def bad_proc(env):
            yield env.timeout(1)
            raise ValueError('process failed')

        p = env.process(bad_proc(env))
        with pytest.raises(ValueError, match='process failed'):
            env.run(until=p)
