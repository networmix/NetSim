import pytest
from netsim.core import (
    SimContext,
    Simulator,
    Event,
    StopSim,
    Process,
)


def test_stop_sim():
    """
    StopSim at time=5 stops the simulation at t=5.
    We verify the final time is 5 and the simulation ended.
    """
    ctx = SimContext()
    sim = Simulator(ctx)
    ctx.schedule_event(StopSim(ctx, delay=5))

    # Generate events at times 0..9
    for i in range(10):
        evt = Event(ctx, time=i, auto_trigger=True)
        ctx.schedule_event(evt)

    sim.run()
    assert ctx.now == 5
    # Up to time=4 => 5 events, plus StopSim => 6 total
    assert sim.event_counter == 6


def test_simulator_avg_event_rate():
    """Verify the average event rate when simulation time advances and events are processed."""
    ctx = SimContext()
    sim = Simulator(ctx)

    for t in range(1, 5):
        evt = Event(ctx, time=t, auto_trigger=True)
        ctx.schedule_event(evt)

    sim.run()
    assert sim.now == 4
    assert sim.event_counter == 4
    assert sim.avg_event_rate == 1.0


def test_simulator_no_events():
    """Confirm that running a simulation with no events finishes immediately."""
    ctx = SimContext()
    sim = Simulator(ctx)
    sim.run()
    assert sim.now == 0
    assert sim.event_counter == 0


def test_simcontext_stop_before_run():
    """
    Ensure stopping the context before any run leaves the simulation at time=0
    and does nothing else.
    """
    ctx = SimContext()
    ctx.stop()
    sim = Simulator(ctx)
    sim.run()
    assert ctx.is_stopped()
    assert ctx.now == 0
    assert sim.event_counter == 0


def test_simulator_final_stats_collection():
    """
    If stat_interval=None, verify that final statistics collection occurs
    after the simulation ends.
    """
    from netsim.core import SimStat

    ctx = SimContext()
    sim = Simulator(ctx, stat_interval=None)

    def p_coro(_ctx):
        for _ in range(5):
            yield ctx.active_process.timeout(1)

    proc = Process(ctx, p_coro(ctx))
    ctx.add_process(proc)

    sim.run()
    assert ctx.now == 5
    proc_name = proc.name
    assert proc_name in sim.stat.process_stat_samples
    intervals = list(sim.stat.process_stat_samples[proc_name].keys())
    assert len(intervals) == 1
    assert intervals[0] == (0, 5)


def test_simulator_run_until_time_only():
    """
    Running simulation with until_time=N (and no other events) should
    end the simulation at time N with one StopSim event.
    """
    ctx = SimContext()
    sim = Simulator(ctx)
    sim.run(until_time=10)
    assert ctx.now == 10
    # Only the StopSim event is processed
    assert sim.event_counter == 1


def test_simulator_run_extra_events_after_until_time():
    """Events scheduled beyond the until_time should not be processed."""
    ctx = SimContext()
    sim = Simulator(ctx)
    e1 = Event(ctx, 9, auto_trigger=True)
    e2 = Event(ctx, 11, auto_trigger=True)
    ctx.schedule_event(e1)
    ctx.schedule_event(e2)

    sim.run(until_time=10)
    assert ctx.now == 10
    # One event at time=9, plus StopSim => total 2 processed
    assert sim.event_counter == 2


def test_simulator_multiple_stat_collectors():
    """Validate simulator operation when multiple StatCollector processes exist."""
    from netsim.core import StatCollector, SimStat

    ctx = SimContext()
    sim = Simulator(ctx, stat_interval=None)

    collector = StatCollector(ctx, stat_interval=2, stat_container=SimStat(ctx))
    sim.add_stat_collector(collector)

    sim.run(until_time=5)
    assert ctx.now == 5


def test_simcontext_copy_noop():
    """Ensure SimContext.__deepcopy__ is effectively a no-op."""
    import copy

    ctx = SimContext(starttime=5)
    cloned = copy.deepcopy(ctx)
    # The __deepcopy__ method in SimContext returns None
    assert cloned is None
    assert ctx.now == 5
