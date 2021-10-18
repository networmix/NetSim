# pylint: disable=protected-access,invalid-name
from collections import deque
import pytest

from netsim.simcore import EventStatus, QueueFIFO, SimContext, Event, Simulator, Process


def test_simcontext_1():
    ctx = SimContext()
    event = Event(ctx, time=1, func=lambda ctx: "TestEvent")
    ctx.schedule_event(event)
    assert ctx.get_event() == event


def test_event_1():
    ctx = SimContext()
    event = Event(ctx, func=lambda ctx: "TestEvent")

    assert event.status == EventStatus.CREATED

    event.plan(time=1)
    assert event.status == EventStatus.PLANNED

    event.schedule()
    assert event.status == EventStatus.SCHEDULED

    event.trigger()
    assert event.status == EventStatus.TRIGGERED


def test_event_2():
    ctx = SimContext()
    event = Event(ctx, time=1, func=lambda ctx: "TestEvent")

    assert event.status == EventStatus.PLANNED

    with pytest.raises(RuntimeError) as e:
        event.plan(time=2)
        assert "It has been already planned" in str(e.value)


def test_simcontext_event_queue_1():
    ctx = SimContext()
    events = [
        Event(ctx, 1, lambda ctx: "TestEvent1"),
        Event(ctx, 2, lambda ctx: "TestEvent2"),
        Event(ctx, 2, lambda ctx: "TestEvent3", priority=1),
    ]

    for event in events:
        ctx.schedule_event(event)

    assert ctx.get_event().time == 1
    assert (itm := ctx.get_event()).time == 2 and itm.priority == 1
    assert (itm := ctx.get_event()).time == 2 and itm.priority == 10


def test_simcontext_stop_1():
    ctx = SimContext()

    assert not ctx.stopped
    ctx.stop()
    assert ctx.stopped


def test_simulator_basics_1():
    assert Simulator()


def test_simulator_basics_2():
    sim = Simulator()
    sim.run()


def test_simulator_basics_3():
    ctx = SimContext()
    sim = Simulator(ctx)

    events = [
        Event(ctx, 1, lambda ctx: "TestEvent1"),
        Event(ctx, 2, lambda ctx: "TestEvent2"),
        Event(ctx, 2, lambda ctx: "TestEvent3", priority=1),
    ]

    for event in events:
        ctx.schedule_event(event)
    sim.run()
    assert ctx.now == 2
    assert sim.event_counter == 3


def test_simulator_basics_4():
    ctx = SimContext()
    sim = Simulator(ctx)

    events = [
        Event(ctx, 1, lambda event: "TestEvent1"),
        Event(ctx, 2, lambda event: "TestEvent2"),
        Event(ctx, 2, lambda event: "TestEvent3", priority=1),
    ]

    def coro(ctx: SimContext):
        for event in events:
            yield event

    proc = Process(ctx, coro(ctx))
    ctx.add_process(proc)

    sim.run()
    assert ctx.now == 2
    assert sim.event_counter == 3


def test_simulator_basics_5():
    ctx = SimContext()
    sim = Simulator(ctx)

    def coro(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)

    proc = Process(ctx, coro(ctx))
    ctx.add_process(proc)

    sim.run()
    assert ctx.now == 10
    assert sim.event_counter == 10


def test_simulator_basics_6():
    ctx = SimContext()
    sim = Simulator(ctx)

    def coro1(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)

    def coro2(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(2)

    ctx.add_process(Process(ctx, coro1(ctx)))
    ctx.add_process(Process(ctx, coro2(ctx)))

    sim.run()
    assert ctx.now == 20
    assert sim.event_counter == 20


def test_simulator_basics_7():
    ctx = SimContext()
    sim = Simulator(ctx)

    def coro1(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)

    def coro2(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(2)

    ctx.add_process(Process(ctx, coro1(ctx)))
    ctx.add_process(Process(ctx, coro2(ctx)))

    sim.run()
    assert ctx.now == 20
    assert sim.event_counter == 20


def test_queue_fifo_put_1():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)
            queue.put(item)

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert len(queue) == 10


def test_queue_fifo_get_1():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx)
    item = "test_item"

    queue._queue = deque([item, item, item])
    queue._queue_runtime_len = len(queue._queue)

    def coro1(ctx: SimContext):
        for _ in range(3):
            ret_item = yield queue.get()
            assert ret_item == item

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert len(queue) == 0
