import pytest
from netsim.core import (
    Event,
    EventStatus,
    EventPriority,
    SimContext,
    Process,
    Timeout,
    CollectStat,
)


def test_simcontext_basic_event_scheduling():
    """Validate that an event can be scheduled and then retrieved in correct order."""
    ctx = SimContext()
    event = Event(ctx, time=1, func=lambda e: "TestEvent")
    ctx.schedule_event(event)

    fetched = ctx.get_event()
    assert fetched is event
    assert fetched.time == 1


def test_simcontext_schedule_into_past_raises():
    """Ensure scheduling an event into a past time triggers a RuntimeError."""
    ctx = SimContext(starttime=10)
    event = Event(ctx, time=9)
    with pytest.raises(RuntimeError, match="into the past"):
        ctx.schedule_event(event)


def test_event_lifecycle():
    """
    Check the full lifecycle transition of an event:
    CREATED -> PLANNED -> SCHEDULED -> TRIGGERED -> PROCESSED.
    """
    ctx = SimContext()
    event = Event(ctx, func=lambda e: "TestEventValue")

    assert event.status == EventStatus.CREATED

    event.plan(time=1)
    assert event.status == EventStatus.PLANNED
    assert event.time == 1

    event.schedule()
    assert event.status == EventStatus.SCHEDULED
    assert event.is_scheduled

    event.trigger()
    assert event.status == EventStatus.TRIGGERED
    assert event.is_triggered

    event.run()
    assert event.status == EventStatus.PROCESSED
    assert event.value == "TestEventValue"


def test_event_plan_already_planned_raises():
    """Confirm that planning an event already in PLANNED state raises RuntimeError."""
    ctx = SimContext()
    event = Event(ctx, time=1)
    assert event.status == EventStatus.PLANNED

    with pytest.raises(RuntimeError, match="already been planned"):
        event.plan(time=2)


def test_event_schedule_already_scheduled_raises():
    """Confirm that scheduling an event already in SCHEDULED state raises RuntimeError."""
    ctx = SimContext()
    event = Event(ctx, time=1)
    event.schedule()
    assert event.status == EventStatus.SCHEDULED

    with pytest.raises(RuntimeError, match="has already been scheduled"):
        event.schedule(time=1)


def test_event_trigger_not_scheduled_raises():
    """Ensure triggering an event that isn't in SCHEDULED state raises RuntimeError."""
    ctx = SimContext()
    event = Event(ctx)
    with pytest.raises(RuntimeError, match="wasn't scheduled"):
        event.trigger()


def test_event_callback_subscribe():
    """
    Verify that subscribing a process to an event causes the process to resume
    when event.run() is called.
    """
    ctx = SimContext()

    def coro(_ctx):
        yield
        return

    proc = Process(ctx, coro(ctx))
    proc.start()
    event = Event(ctx, time=0)
    event.subscribe(proc)

    # Schedule and trigger the event so run() occurs
    event.schedule()
    event.trigger()
    event.run()

    # Process got a single yield => should be stopped
    assert proc.is_stopped()


def test_event_callback_arbitrary():
    """Confirm arbitrary callbacks added to an event are invoked upon event.run()."""
    ctx = SimContext()
    called_flags = []

    def callback(_evt):
        called_flags.append(True)

    event = Event(ctx, time=0)
    event.add_callback(callback)
    event.schedule()
    event.trigger()
    event.run()

    assert len(called_flags) == 1


def test_timeout_auto_trigger():
    """Check that a Timeout event sets itself to triggered as soon as it's scheduled."""
    ctx = SimContext()
    timeout_event = Timeout(ctx, delay=5)
    assert timeout_event._auto_trigger is True

    timeout_event.schedule()
    assert timeout_event.is_triggered


def test_collect_stat():
    """Validate that a CollectStat event auto-triggers and can be scheduled before simulation stop."""
    ctx = SimContext()
    cstat1 = CollectStat(ctx, delay=2)
    cstat2 = CollectStat(ctx, delay=4)

    ctx.schedule_event(cstat1)
    ctx.schedule_event(cstat2)

    # Just fetch them to ensure they are scheduled in ascending order
    e1 = ctx.get_event()
    e2 = ctx.get_event()
    assert e1.time == 2
    assert e2.time == 4
    # Both are triggered due to auto_trigger
    assert e1.is_triggered
    assert e2.is_triggered
