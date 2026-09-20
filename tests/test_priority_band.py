"""The ``DEFERRED`` priority band: settle work runs after all ``NORMAL``
work at the same simulation time, including ``NORMAL`` events scheduled
while that time is being processed."""

import netsim
from netsim import core


def _deferred_event(env, callback, priority=core.DEFERRED, delay=0):
    """Schedule a pre-triggered event at *priority* the way ``run(until)``
    schedules its stop event, without touching ``Timeout``."""
    ev = core.Event(env)
    ev._triggered = True
    ev._ok = True
    ev.callbacks.append(callback)
    env.schedule(ev, priority, delay)
    return ev


class TestPriorityBand:
    def test_band_constants_are_ordered(self):
        assert core.URGENT < core.NORMAL < core.DEFERRED
        assert netsim.DEFERRED is core.DEFERRED

    def test_deferred_runs_after_normal_scheduled_during_processing(self):
        env = netsim.Environment()
        order = []

        def normal_second(ev):
            order.append('normal-2')

        def normal_first(ev):
            order.append('normal-1')
            # A NORMAL event scheduled *while* time 0 is being processed
            # still precedes the deferred event already in the heap.
            env.timeout(0).callbacks.append(normal_second)

        _deferred_event(env, lambda ev: order.append('deferred'))
        env.timeout(0).callbacks.append(normal_first)
        env.run()
        assert order == ['normal-1', 'normal-2', 'deferred']

    def test_deferred_band_is_fifo_by_priority_then_eid(self):
        env = netsim.Environment()
        order = []
        _deferred_event(env, lambda ev: order.append('b+1'), core.DEFERRED + 1)
        _deferred_event(env, lambda ev: order.append('b+0 first'), core.DEFERRED)
        _deferred_event(env, lambda ev: order.append('b+0 second'), core.DEFERRED)
        env.run()
        assert order == ['b+0 first', 'b+0 second', 'b+1']

    def test_deferred_at_later_time_does_not_preempt_earlier_normal(self):
        env = netsim.Environment()
        order = []
        _deferred_event(env, lambda ev: order.append(('deferred', env.now)), delay=1)
        env.timeout(2).callbacks.append(lambda ev: order.append(('normal', env.now)))
        env.run()
        assert order == [('deferred', 1), ('normal', 2)]

    def test_run_until_time_stops_before_deferred_work_at_that_time(self):
        """``run(until=t)`` inserts an URGENT stop event, so deferred work at
        exactly ``t`` has not run when ``run`` returns."""
        env = netsim.Environment()
        ran = []
        _deferred_event(env, lambda ev: ran.append(env.now), delay=5)
        env.run(until=5)
        assert ran == []
        env.run()
        assert ran == [5]
