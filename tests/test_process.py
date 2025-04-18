import pytest
from netsim.core import (
    SimContext,
    Process,
    Timeout,
    Event,
    ProcessStatus,
)


def test_process_basic_lifecycle():
    """
    Ensure a process transitions from CREATED to RUNNING when started,
    using a simple coroutine with multiple yields.
    """
    ctx = SimContext()

    def coro(_ctx):
        yield Event(ctx, time=1)
        yield Event(ctx, time=2)

    proc = Process(ctx, coro(ctx))
    assert proc.status == ProcessStatus.CREATED
    proc.start()
    assert proc.status == ProcessStatus.RUNNING


def test_process_resume_incorrect_timeout():
    """Verify that resuming a process with a mismatched timeout event raises RuntimeError."""
    ctx = SimContext()

    def coro(_ctx):
        yield ctx.active_process.timeout(5)
        return

    proc = Process(ctx, coro(ctx))
    proc.start()

    # Create a different event that does not match proc's timeout
    other_event = Event(ctx, time=5)
    with pytest.raises(RuntimeError, match="Wrong .* to resume the process in timeout"):
        proc.resume(other_event)


def test_process_subscribe():
    """
    Check that multiple processes can subscribe to a single process's generated event.
    The second process should resume once and then stop.
    """
    ctx = SimContext()

    def p1_coro(_ctx):
        evt = Event(ctx, time=0)
        yield evt  # after this yields, p1 stops

    def p2_coro(_ctx):
        # We'll yield once; on the second resume, we actually stop
        dummy_event = yield
        return dummy_event

    p1 = Process(ctx, p1_coro(ctx))
    p2 = Process(ctx, p2_coro(ctx))

    p1.subscribe(p2)
    p1.start()
    p2.start()

    evt = ctx.get_event()  # event from p1
    evt.trigger()
    evt.run()

    # By now, p2 has resumed once and should be STOPPED
    assert p2.status == ProcessStatus.STOPPED


def test_process_with_no_events():
    """A process that yields no events stops immediately upon start()."""
    ctx = SimContext()

    def p_coro(_ctx):
        return
        yield  # never reached

    proc = Process(ctx, p_coro(ctx))
    proc.start()
    assert proc.is_stopped()


def test_timeout_resume_mechanic():
    """
    Check that a process yielding a timeout can only be resumed by that specific timeout event.
    """
    ctx = SimContext()

    def p_coro(_ctx):
        yield ctx.active_process.timeout(3)

    proc = Process(ctx, p_coro(ctx))
    proc.start()

    e = ctx.get_event()
    assert isinstance(e, Timeout)
    assert e.process is proc

    other_evt = Event(ctx, time=3)
    with pytest.raises(RuntimeError, match="Wrong .* to resume"):
        proc.resume(other_evt)


def test_subscriber_mechanic_multiple():
    """
    Multiple processes subscribed to a single process's events
    should each receive resume calls.
    """
    ctx = SimContext()

    def main_coro(_ctx):
        for i in range(3):
            yield Event(ctx, time=ctx.now, auto_trigger=True, func=lambda e, i=i: i)

    def side_coro(_ctx, collector):
        while True:
            val = yield
            collector.append(val)

    collector1 = []
    collector2 = []

    p_main = Process(ctx, main_coro(ctx), name="main")
    p_side1 = Process(ctx, side_coro(ctx, collector1), name="side1")
    p_side2 = Process(ctx, side_coro(ctx, collector2), name="side2")

    # Subscribe side processes to main, so they get resumed by main's events
    p_main.subscribe(p_side1)
    p_main.subscribe(p_side2)

    # Start all
    p_main.start()
    p_side1.start()
    p_side2.start()

    # Process the queued events
    while True:
        evt = ctx.get_event()
        if not evt:
            break
        evt.run()

    # Each side process should have collected 3 values
    assert len(collector1) == 3
    assert len(collector2) == 3
