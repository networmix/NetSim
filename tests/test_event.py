"""Tests for Event creation, triggering, callbacks, and state transitions."""

import pytest

import netsim


class TestEventCreate:
    def test_new_event_is_pending(self):
        env = netsim.Environment()
        evt = env.event()
        assert not evt.triggered
        assert not evt.processed
        assert not evt.ok

    def test_event_belongs_to_env(self):
        env = netsim.Environment()
        evt = env.event()
        assert evt.env is env


class TestEventSucceed:
    def test_succeed_sets_triggered(self):
        env = netsim.Environment()
        evt = env.event()
        evt.succeed(value=42)
        assert evt.triggered
        assert evt.ok

    def test_succeed_with_default_value(self):
        env = netsim.Environment()
        evt = env.event()
        evt.succeed()
        env.step()
        assert evt.value is None

    def test_succeed_with_value(self):
        env = netsim.Environment()
        evt = env.event()
        evt.succeed(value='hello')
        env.step()
        assert evt.value == 'hello'

    def test_succeed_returns_self(self):
        env = netsim.Environment()
        evt = env.event()
        result = evt.succeed()
        assert result is evt


class TestEventFail:
    def test_fail_sets_triggered(self):
        env = netsim.Environment()
        evt = env.event()
        evt.fail(ValueError('boom'))
        assert evt.triggered
        assert not evt.ok

    def test_fail_value_is_exception(self):
        env = netsim.Environment()
        evt = env.event()
        exc = ValueError('boom')
        evt.fail(exc)
        assert evt.value is exc

    def test_fail_requires_exception_instance(self):
        env = netsim.Environment()
        evt = env.event()
        with pytest.raises(TypeError):
            evt.fail('not an exception')

    def test_fail_returns_self(self):
        env = netsim.Environment()
        evt = env.event()
        result = evt.fail(RuntimeError('oops'))
        assert result is evt

    def test_undefused_failed_event_raises_on_step(self):
        env = netsim.Environment()
        evt = env.event()
        evt.fail(ValueError('boom'))
        with pytest.raises(ValueError, match='boom'):
            env.step()


class TestEventTrigger:
    def test_trigger_chains_success(self):
        env = netsim.Environment()
        source = env.event()
        target = env.event()
        source.callbacks.append(target.trigger)
        source.succeed(value=99)
        env.run()
        assert target.triggered
        assert target.ok
        assert target.value == 99

    def test_trigger_chains_failure(self):
        env = netsim.Environment()
        source = env.event()
        target = env.event()
        source.callbacks.append(target.trigger)
        exc = RuntimeError('fail')
        source.fail(exc)
        # Defuse the source so stepping doesn't raise, then defuse target too
        source.defused = True
        target.defused = True
        env.run()
        assert target.triggered
        assert not target.ok
        assert target.value is exc

    def test_double_trigger_raises(self):
        env = netsim.Environment()
        evt = env.event()
        evt.succeed()
        with pytest.raises(RuntimeError):
            evt.succeed()

    def test_double_fail_raises(self):
        env = netsim.Environment()
        evt = env.event()
        evt.fail(ValueError('first'))
        with pytest.raises(RuntimeError):
            evt.fail(ValueError('second'))

    def test_succeed_after_fail_raises(self):
        env = netsim.Environment()
        evt = env.event()
        evt.fail(ValueError('first'))
        with pytest.raises(RuntimeError):
            evt.succeed()


class TestEventCallbacks:
    def test_callbacks_executed_on_processing(self):
        env = netsim.Environment()
        evt = env.event()
        results = []
        evt.callbacks.append(lambda e: results.append(e.value))
        evt.succeed(value=10)
        env.step()
        assert results == [10]

    def test_callbacks_set_to_none_after_processing(self):
        env = netsim.Environment()
        evt = env.event()
        evt.succeed()
        env.step()
        assert evt.callbacks is None
        assert evt.processed

    def test_multiple_callbacks_all_called(self):
        env = netsim.Environment()
        evt = env.event()
        called = []
        evt.callbacks.append(lambda e: called.append('a'))
        evt.callbacks.append(lambda e: called.append('b'))
        evt.succeed()
        env.step()
        assert called == ['a', 'b']


class TestEventValueAccess:
    def test_value_before_trigger_raises_attribute_error(self):
        env = netsim.Environment()
        evt = env.event()
        with pytest.raises(AttributeError):
            _ = evt.value


class TestEventDefused:
    def test_defused_flag_default(self):
        env = netsim.Environment()
        evt = env.event()
        assert not evt.defused

    def test_defused_flag_can_be_set(self):
        env = netsim.Environment()
        evt = env.event()
        evt.defused = True
        assert evt.defused

    def test_defused_failed_event_does_not_raise(self):
        env = netsim.Environment()
        evt = env.event()
        evt.fail(ValueError('boom'))
        evt.defused = True
        # Should not raise
        env.step()
        assert evt.processed


class TestTriggerDoubleTriggerRaises:
    def test_trigger_double_trigger_raises(self):
        """Calling event.trigger(source) on an already-triggered event raises RuntimeError."""
        env = netsim.Environment()
        source = env.event()
        target = env.event()
        source.succeed(value=1)
        # Trigger target via trigger()
        target.trigger(source)
        assert target.triggered
        # Now trigger again on the already-triggered target -- should raise
        source2 = env.event()
        source2.succeed(value=2)
        with pytest.raises(RuntimeError):
            target.trigger(source2)


class TestTriggerUsedAsCallbackChaining:
    def test_trigger_used_as_callback_chaining(self):
        """Register target.trigger as a callback on source; after step, target has same value."""
        env = netsim.Environment()
        source = env.event()
        target = env.event()
        source.callbacks.append(target.trigger)
        source.succeed(value='chained')
        env.step()  # process source, which fires target.trigger callback
        assert target.triggered
        assert target.ok
        env.step()  # process target
        assert target.value == 'chained'
