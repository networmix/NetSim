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


class TestRunUntilRunsAllCallbacks:
    def test_stop_does_not_leave_waiters_stranded(self):
        """Stopping at the *until* event must not skip its other callbacks:
        a process that started waiting on it during run() is still resumed."""
        env = netsim.Environment()
        log = []

        def worker(env):
            yield env.timeout(2)
            return 'done'

        def starter(env, w):
            yield env.timeout(1)
            env.process(waiter(env, w))

        def waiter(env, w):
            v = yield w
            log.append(v)

        w = env.process(worker(env))
        env.process(starter(env, w))
        assert env.run(until=w) == 'done'
        assert log == ['done']
        assert w.processed

    def test_stale_until_event_does_not_stop_a_later_run(self):
        """An *until* event left over from an aborted run() is an ordinary
        event for later runs; only the current call's *until* stops it."""
        env = netsim.Environment()

        def crasher(env):
            yield env.timeout(1)
            raise ValueError('boom')

        def ticker(env):
            while True:
                yield env.timeout(1)

        env.process(crasher(env))
        env.process(ticker(env))
        with pytest.raises(ValueError):
            env.run(until=3)
        env.run(until=10)
        assert env.now == 10


class TestRunUntilProcessedFailedEventRaises:
    def test_run_until_processed_failed_event_raises(self):
        env = netsim.Environment()
        evt = env.event()
        evt.fail(ValueError('boom'))
        evt.defused = True
        env.step()
        with pytest.raises(ValueError, match='boom'):
            env.run(until=evt)

    def test_run_until_defused_failed_event_raises(self):
        """Even if a process handles the failure, run(until=evt) reports it."""
        env = netsim.Environment()
        evt = env.event()

        def handler(env):
            try:
                yield evt
            except ValueError:
                pass

        env.process(handler(env))
        evt.fail(ValueError('boom'))
        with pytest.raises(ValueError, match='boom'):
            env.run(until=evt)


class TestRunUntilValidation:
    def test_until_nan_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='must be >'):
            env.run(until=float('nan'))

    def test_until_event_of_other_env_raises(self):
        env1 = netsim.Environment()
        env2 = netsim.Environment()
        with pytest.raises(ValueError, match='different environment'):
            env1.run(until=env2.event())


class TestStepReturnsEvent:
    def test_step_returns_processed_event(self):
        env = netsim.Environment()
        t = env.timeout(3, value='x')
        assert env.step() is t
        assert t.processed


class TestRunMatchesStep:
    """run() inlines step(); both must process the same events in the same
    order with the same clock."""

    @staticmethod
    def _scenario(env, trace):
        store = netsim.Store(env, capacity=1)
        res = netsim.Resource(env, capacity=1)

        def producer(env):
            for i in range(5):
                yield store.put(i)
                trace.append(('put', i, env.now))
                yield env.timeout(1)

        def consumer(env):
            for _ in range(5):
                item = yield store.get() | env.timeout(3)
                trace.append(('got', list(item.values()), env.now))
                with res.request() as r:
                    yield r
                    yield env.timeout(2)

        def victim(env):
            try:
                yield env.timeout(100)
            except netsim.Interrupt as e:
                trace.append(('interrupted', e.cause, env.now))

        def attacker(env, v):
            yield env.timeout(4)
            v.interrupt('hi')

        env.process(producer(env))
        env.process(consumer(env))
        v = env.process(victim(env))
        env.process(attacker(env, v))

    def test_run_and_step_loop_produce_identical_traces(self):
        env_run = netsim.Environment()
        trace_run = []
        self._scenario(env_run, trace_run)
        env_run.run()

        env_step = netsim.Environment()
        trace_step = []
        self._scenario(env_step, trace_step)
        with pytest.raises(netsim.EmptySchedule):
            while True:
                env_step.step()

        assert trace_run == trace_step
        assert env_run.now == env_step.now
