"""Tests for Process lifecycle, yielding, and properties."""

import pytest

import netsim


class TestProcessStartAndYield:
    def test_process_starts_immediately(self):
        env = netsim.Environment()
        started = []

        def proc(env):
            started.append(True)
            yield env.timeout(0)

        env.process(proc(env))
        env.run()
        assert started == [True]

    def test_process_yields_timeout(self):
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(5)
            yield env.timeout(3)

        env.process(proc(env))
        env.run()
        assert env.now == 8


class TestProcessResumeWithValue:
    def test_yield_returns_event_value(self):
        env = netsim.Environment()
        received = []

        def proc(env):
            val = yield env.timeout(1, value=42)
            received.append(val)

        env.process(proc(env))
        env.run()
        assert received == [42]


class TestProcessReturn:
    def test_return_value_via_stop_iteration(self):
        env = netsim.Environment()

        def child(env):
            yield env.timeout(1)
            return 'done'

        def parent(env):
            result = yield env.process(child(env))
            assert result == 'done'

        env.process(parent(env))
        env.run()

    def test_process_is_event_yield_gets_return(self):
        """Process IS Event: yielding a process from another gets the return value."""
        env = netsim.Environment()
        results = []

        def worker(env):
            yield env.timeout(2)
            return 100

        def boss(env):
            val = yield env.process(worker(env))
            results.append(val)

        env.process(boss(env))
        env.run()
        assert results == [100]
        assert env.now == 2


class TestProcessExceptionPropagation:
    def test_generator_exception_propagates(self):
        env = netsim.Environment()

        def bad_proc(env):
            yield env.timeout(1)
            raise RuntimeError('process error')

        env.process(bad_proc(env))
        with pytest.raises(RuntimeError, match='process error'):
            env.run()


class TestProcessName:
    def test_explicit_name(self):
        env = netsim.Environment()

        def my_gen(env):
            yield env.timeout(0)

        p = env.process(my_gen(env), name='custom')
        assert p.name == 'custom'

    def test_name_from_generator(self):
        env = netsim.Environment()

        def my_generator(env):
            yield env.timeout(0)

        p = env.process(my_generator(env))
        assert p.name == 'my_generator'


class TestProcessIsAlive:
    def test_is_alive_before_completion(self):
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(10)

        p = env.process(proc(env))
        assert p.is_alive

    def test_is_alive_after_completion(self):
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(1)

        p = env.process(proc(env))
        env.run()
        assert not p.is_alive


class TestProcessTarget:
    def test_target_property(self):
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(5)

        p = env.process(proc(env))
        # After init, target should be an event (the timeout).
        # We need to step past _Initialize first.
        env.step()
        assert p.target is not None


class TestProcessNotAGenerator:
    def test_non_generator_raises(self):
        env = netsim.Environment()
        with pytest.raises(ValueError, match='is not a generator'):
            env.process('not a generator')


class TestProcessYieldsAlreadyProcessedEvent:
    def test_process_yields_already_processed_event(self):
        """A process that yields an already-processed event should immediately
        receive the value without blocking."""
        env = netsim.Environment()
        results = []

        def proc(env):
            # Create a timeout and let it be processed
            t = env.timeout(0, value='immediate')
            yield env.timeout(1)  # wait so t is processed
            # Now t is already processed (callbacks is None)
            val = yield t
            results.append(val)

        env.process(proc(env))
        env.run()
        assert results == ['immediate']


class TestProcessYieldsNonEventRaisesRuntimeError:
    def test_process_yields_non_event_raises_runtime_error(self):
        """A process that yields a string (not an Event) should raise RuntimeError."""
        env = netsim.Environment()

        def bad_proc(env):
            yield 'not an event'

        env.process(bad_proc(env))
        with pytest.raises(RuntimeError, match='Invalid yield value'):
            env.run()


class TestProcessGeneratorClearedAfterTermination:
    def test_process_generator_cleared_after_termination(self):
        """After a process terminates, process._generator should be None."""
        env = netsim.Environment()

        def proc(env):
            yield env.timeout(1)

        p = env.process(proc(env))
        env.run()
        assert not p.is_alive
        assert p._generator is None


class TestProcessNameAfterGeneratorCleared:
    def test_process_name_after_generator_cleared(self):
        """After termination with no explicit name, process.name should not crash."""
        env = netsim.Environment()

        def my_proc(env):
            yield env.timeout(1)

        p = env.process(my_proc(env))
        env.run()
        assert p._generator is None
        # Should return fallback string, not crash
        name = p.name
        assert isinstance(name, str)
        assert len(name) > 0
