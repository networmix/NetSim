"""Tests for AllOf, AnyOf, Condition, and ConditionValue."""

import pytest

import netsim


class TestAllOf:
    def test_all_events_must_trigger(self):
        env = netsim.Environment()
        results = []

        def proc(env):
            e1 = env.timeout(2, value='a')
            e2 = env.timeout(5, value='b')
            val = yield env.all_of([e1, e2])
            results.append(val)

        env.process(proc(env))
        env.run()
        assert env.now == 5
        assert len(results) == 1
        cv = results[0]
        assert list(cv.values()) == ['a', 'b']


class TestAnyOf:
    def test_any_event_triggers(self):
        env = netsim.Environment()
        results = []
        times = []

        def proc(env):
            e1 = env.timeout(2, value='first')
            e2 = env.timeout(10, value='second')
            val = yield env.any_of([e1, e2])
            times.append(env.now)
            results.append(val)

        env.process(proc(env))
        env.run()
        assert times[0] == 2
        cv = results[0]
        assert list(cv.values()) == ['first']


class TestNestedConditions:
    def test_nested_all_of(self):
        env = netsim.Environment()
        results = []

        def proc(env):
            e1 = env.timeout(1, value='x')
            e2 = env.timeout(2, value='y')
            e3 = env.timeout(3, value='z')
            inner = e1 & e2
            outer = inner & e3
            val = yield outer
            results.append(list(val.values()))

        env.process(proc(env))
        env.run()
        assert results == [['x', 'y', 'z']]


class TestOperators:
    def test_and_operator(self):
        env = netsim.Environment()
        results = []

        def proc(env):
            e1 = env.timeout(1, value=10)
            e2 = env.timeout(2, value=20)
            val = yield e1 & e2
            results.append(list(val.values()))

        env.process(proc(env))
        env.run()
        assert results == [[10, 20]]

    def test_or_operator(self):
        env = netsim.Environment()
        results = []

        def proc(env):
            e1 = env.timeout(1, value=10)
            e2 = env.timeout(100, value=20)
            val = yield e1 | e2
            results.append(list(val.values()))

        env.process(proc(env))
        env.run()
        assert results == [[10]]


class TestEmptyCondition:
    def test_empty_allof_succeeds_immediately(self):
        env = netsim.Environment()
        results = []

        def proc(env):
            yield env.all_of([])
            results.append('done')

        env.process(proc(env))
        env.run()
        assert results == ['done']
        assert env.now == 0

    def test_empty_anyof_succeeds_immediately(self):
        env = netsim.Environment()
        results = []

        def proc(env):
            yield env.any_of([])
            results.append('done')

        env.process(proc(env))
        env.run()
        assert results == ['done']


class TestFailedEventInCondition:
    def test_failed_event_fails_condition(self):
        env = netsim.Environment()
        caught = []

        def proc(env):
            e1 = env.event()
            e2 = env.timeout(5)
            cond = e1 & e2
            e1.fail(ValueError('oops'))
            try:
                yield cond
            except ValueError as e:
                caught.append(str(e))

        env.process(proc(env))
        env.run()
        assert caught == ['oops']


class TestConditionValue:
    def test_keys_values_items(self):
        env = netsim.Environment()
        results = {}

        def proc(env):
            e1 = env.timeout(1, value='a')
            e2 = env.timeout(2, value='b')
            cv = yield e1 & e2
            results['keys'] = list(cv.keys())
            results['values'] = list(cv.values())
            results['items'] = list(cv.items())

        env.process(proc(env))
        env.run()
        assert len(results['keys']) == 2
        assert results['values'] == ['a', 'b']
        assert len(results['items']) == 2
        for evt, _val in results['items']:
            assert isinstance(evt, netsim.Event)

    def test_getitem(self):
        env = netsim.Environment()

        def proc(env):
            e1 = env.timeout(1, value='hello')
            e2 = env.timeout(2, value='world')
            cv = yield e1 & e2
            assert cv[e1] == 'hello'
            assert cv[e2] == 'world'

        env.process(proc(env))
        env.run()

    def test_getitem_missing_key_raises(self):
        env = netsim.Environment()

        def proc(env):
            e1 = env.timeout(1, value='a')
            e2 = env.timeout(5, value='b')
            cv = yield env.any_of([e1, e2])
            with pytest.raises(KeyError):
                _ = cv[e2]

        env.process(proc(env))
        env.run()

    def test_contains(self):
        env = netsim.Environment()

        def proc(env):
            e1 = env.timeout(1, value='a')
            e2 = env.timeout(100, value='b')
            cv = yield env.any_of([e1, e2])
            assert e1 in cv
            assert e2 not in cv

        env.process(proc(env))
        env.run()

    def test_todict(self):
        env = netsim.Environment()
        result = {}

        def proc(env):
            e1 = env.timeout(1, value=10)
            e2 = env.timeout(2, value=20)
            cv = yield e1 & e2
            d = cv.todict()
            result['dict'] = d

        env.process(proc(env))
        env.run()
        assert len(result['dict']) == 2
        assert list(result['dict'].values()) == [10, 20]


class TestMixedEnvironmentsRaises:
    def test_mixed_environments_raises(self):
        env1 = netsim.Environment()
        env2 = netsim.Environment()
        e1 = env1.event()
        e2 = env2.event()
        with pytest.raises(ValueError, match='different environments'):
            netsim.AllOf(env1, [e1, e2])


class TestConditionCheckSkipsAfterAlreadyTriggered:
    def test_condition_check_skips_after_already_triggered(self):
        """AnyOf with two events. Trigger both. The second _check call should
        be a no-op (no error, condition value unchanged)."""
        env = netsim.Environment()
        results = []

        def proc(env):
            e1 = env.timeout(1, value='first')
            e2 = env.timeout(1, value='second')
            cv = yield env.any_of([e1, e2])
            results.append(list(cv.values()))

        env.process(proc(env))
        env.run()
        # AnyOf triggers on the first event; the second _check is a no-op
        assert len(results) == 1
        # The condition was triggered by the first event only
        assert len(results[0]) >= 1


class TestConditionValueEqWithDict:
    def test_condition_value_eq_with_dict(self):
        """cv == cv.todict() should be True."""
        env = netsim.Environment()
        results = []

        def proc(env):
            e1 = env.timeout(1, value='a')
            e2 = env.timeout(2, value='b')
            cv = yield e1 & e2
            results.append(cv == cv.todict())

        env.process(proc(env))
        env.run()
        assert results == [True]


class TestConditionValueEqWithAnotherCV:
    def test_condition_value_eq_with_another_cv(self):
        """Two CVs with same events should be equal."""
        cv1 = netsim.ConditionValue()
        cv2 = netsim.ConditionValue()
        env = netsim.Environment()
        e = env.event()
        e.succeed(value=42)
        cv1.events.append(e)
        cv2.events.append(e)
        assert cv1 == cv2


class TestConditionValueEqWithUnrelatedType:
    def test_condition_value_eq_with_unrelated_type(self):
        """cv.__eq__(42) returns NotImplemented."""
        cv = netsim.ConditionValue()
        assert cv.__eq__(42) is NotImplemented


class TestConditionValueIter:
    def test_condition_value_iter(self):
        """list(cv) should return the events."""
        cv = netsim.ConditionValue()
        env = netsim.Environment()
        e1 = env.event()
        e2 = env.event()
        cv.events.append(e1)
        cv.events.append(e2)
        assert list(cv) == [e1, e2]
