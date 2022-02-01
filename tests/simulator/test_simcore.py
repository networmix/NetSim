# pylint: disable=protected-access,invalid-name
import pytest

from netsim.simulator.simcore import (
    EventStatus,
    QueueFIFO,
    SimContext,
    Event,
    Simulator,
    Process,
)


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

    assert not ctx.is_stopped()
    ctx.stop()
    assert ctx.is_stopped()


def test_simulator_basics_1():
    assert Simulator()


def test_simulator_basics_2():
    sim = Simulator()
    sim.run()

    assert sim.now == 0


def test_simulator_basics_3():
    ctx = SimContext()
    sim = Simulator(ctx)

    events = [
        Event(ctx, 1, lambda ctx: "TestEvent1", auto_trigger=True),
        Event(ctx, 2, lambda ctx: "TestEvent2", auto_trigger=True),
        Event(ctx, 2, lambda ctx: "TestEvent3", priority=1, auto_trigger=True),
    ]

    for event in events:
        ctx.schedule_event(event)
    sim.run()
    assert ctx.now == 2
    assert sim.now == ctx.now
    assert sim.event_counter == 3


def test_simulator_basics_4():
    ctx = SimContext()
    sim = Simulator(ctx)

    events = [
        Event(ctx, 1, lambda event: "TestEvent1", auto_trigger=True),
        Event(ctx, 2, lambda event: "TestEvent2", auto_trigger=True),
        Event(ctx, 2, lambda event: "TestEvent3", priority=1, auto_trigger=True),
    ]

    def coro(_: SimContext):
        for event in events:
            yield event

    proc = Process(ctx, coro(ctx))
    proc.add_stat_callback(proc.stat.advance_time)
    ctx.add_process(proc)

    sim.run()
    assert ctx.now == 2
    assert sim.event_counter == 3

    assert proc.stat.todict() == {
        "cur_interval_duration": 2,
        "cur_stat_frame": {
            "avg_event_exec_rate": 1.5,
            "avg_event_gen_rate": 1.5,
            "duration": 2,
            "last_state_change_timestamp": 2,
            "timestamp": 2,
            "total_event_exec_count": 3,
            "total_event_gen_count": 3,
        },
        "cur_timestamp": 2,
        "last_state_change_timestamp": 2,
        "prev_stat_frame": {
            "avg_event_exec_rate": 1.0,
            "avg_event_gen_rate": 2.0,
            "duration": 1,
            "last_state_change_timestamp": 1,
            "timestamp": 1,
            "total_event_exec_count": 1,
            "total_event_gen_count": 2,
        },
        "prev_timestamp": 1,
        "start_interval_timestamp": 0,
    }


def test_simulator_basics_5():
    ctx = SimContext()
    sim = Simulator(ctx)

    def coro(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)

    proc = Process(ctx, coro(ctx))
    proc.add_stat_callback(proc.stat.advance_time)
    ctx.add_process(proc)

    sim.run()
    assert ctx.now == 10
    assert sim.event_counter == 10
    assert sim.avg_event_rate == 1
    assert proc.stat.cur_timestamp == 10

    assert proc.stat.todict() == {
        "cur_interval_duration": 10,
        "cur_stat_frame": {
            "avg_event_exec_rate": 1.0,
            "avg_event_gen_rate": 1.0,
            "duration": 10,
            "last_state_change_timestamp": 10,
            "timestamp": 10,
            "total_event_exec_count": 10,
            "total_event_gen_count": 10,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 10,
        "prev_stat_frame": {
            "avg_event_exec_rate": 1.0,
            "avg_event_gen_rate": 1.1111111111111112,
            "duration": 9,
            "last_state_change_timestamp": 9,
            "timestamp": 9,
            "total_event_exec_count": 9,
            "total_event_gen_count": 10,
        },
        "prev_timestamp": 9,
        "start_interval_timestamp": 0,
    }


def test_simulator_basics_6():
    ctx = SimContext()
    sim = Simulator(ctx)

    def coro1(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)

    def coro2(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(2)

    proc1 = Process(ctx, coro1(ctx))
    proc2 = Process(ctx, coro2(ctx))

    ctx.add_process(proc1)
    proc1.add_stat_callback(proc1.stat.advance_time)
    ctx.add_process(proc2)
    proc2.add_stat_callback(proc2.stat.advance_time)

    sim.run()
    assert ctx.now == 20
    assert sim.event_counter == 20
    assert proc1.stat.cur_timestamp == 20
    assert proc2.stat.cur_timestamp == 20

    assert proc1.stat.todict() == {
        "cur_interval_duration": 20,
        "cur_stat_frame": {
            "avg_event_exec_rate": 0.5,
            "avg_event_gen_rate": 0.5,
            "duration": 20,
            "last_state_change_timestamp": 10,
            "timestamp": 20,
            "total_event_exec_count": 10,
            "total_event_gen_count": 10,
        },
        "cur_timestamp": 20,
        "last_state_change_timestamp": 10,
        "prev_stat_frame": {
            "avg_event_exec_rate": 0.5555555555555556,
            "avg_event_gen_rate": 0.5555555555555556,
            "duration": 18,
            "last_state_change_timestamp": 10,
            "timestamp": 18,
            "total_event_exec_count": 10,
            "total_event_gen_count": 10,
        },
        "prev_timestamp": 18,
        "start_interval_timestamp": 0,
    }

    assert proc2.stat.todict() == {
        "cur_interval_duration": 20,
        "cur_stat_frame": {
            "avg_event_exec_rate": 0.5,
            "avg_event_gen_rate": 0.5,
            "duration": 20,
            "last_state_change_timestamp": 20,
            "timestamp": 20,
            "total_event_exec_count": 10,
            "total_event_gen_count": 10,
        },
        "cur_timestamp": 20,
        "last_state_change_timestamp": 20,
        "prev_stat_frame": {
            "avg_event_exec_rate": 0.5,
            "avg_event_gen_rate": 0.5555555555555556,
            "duration": 18,
            "last_state_change_timestamp": 18,
            "timestamp": 18,
            "total_event_exec_count": 9,
            "total_event_gen_count": 10,
        },
        "prev_timestamp": 18,
        "start_interval_timestamp": 0,
    }


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


def test_simulator_stat_1():
    ctx = SimContext()
    sim = Simulator(ctx, stat_interval=3)

    events = [
        Event(ctx, 0, lambda event: "TestEvent1", auto_trigger=True),
        Event(ctx, 1, lambda event: "TestEvent2", auto_trigger=True),
        Event(ctx, 2, lambda event: "TestEvent3", auto_trigger=True),
        Event(ctx, 3, lambda event: "TestEvent4", auto_trigger=True),
        Event(ctx, 4, lambda event: "TestEvent5", auto_trigger=True),
        Event(ctx, 5, lambda event: "TestEvent6", auto_trigger=True),
    ]

    def coro(_: SimContext):
        for event in events:
            yield event

    proc = Process(ctx, coro(ctx))
    ctx.add_process(proc)
    proc.add_stat_callback(proc.stat.advance_time)

    sim.run(until_time=6)

    assert ctx.now == 6
    assert sim.event_counter == 9
    assert sim.stat.todict() == {
        "process_stat_samples": {
            "Process_2": {
                (0, 3): {
                    "avg_event_exec_rate": 1.0,
                    "avg_event_gen_rate": 1.3333333333333333,
                    "duration": 3,
                    "last_state_change_timestamp": 2,
                    "timestamp": 3,
                    "total_event_exec_count": 3,
                    "total_event_gen_count": 4,
                },
                (3, 6): {
                    "avg_event_exec_rate": 1.0,
                    "avg_event_gen_rate": 0.6666666666666666,
                    "duration": 3,
                    "last_state_change_timestamp": 5,
                    "timestamp": 6,
                    "total_event_exec_count": 3,
                    "total_event_gen_count": 2,
                },
            },
            "StatCollector_1": {
                (0, 3): {
                    "avg_event_exec_rate": 0.3333333333333333,
                    "avg_event_gen_rate": 0.3333333333333333,
                    "duration": 3,
                    "last_state_change_timestamp": 3,
                    "timestamp": 3,
                    "total_event_exec_count": 1,
                    "total_event_gen_count": 1,
                },
                (3, 6): {
                    "avg_event_exec_rate": 0.3333333333333333,
                    "avg_event_gen_rate": 0.3333333333333333,
                    "duration": 3,
                    "last_state_change_timestamp": 6,
                    "timestamp": 6,
                    "total_event_exec_count": 1,
                    "total_event_gen_count": 1,
                },
            },
        }
    }


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
    assert ctx.now == 10
    assert len(queue) == 10
    assert sim.event_counter == 20


def test_queue_fifo_put_2():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(10):
            queue.put(item)
            yield ctx.active_process.timeout(1)
            queue.put(item)

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert ctx.now == 10
    assert len(queue) == 20
    assert sim.event_counter == 30


def test_queue_fifo_put_3():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx, capacity=2)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(10):
            yield ctx.active_process.timeout(1)
            queue.put(item)

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert ctx.now == 10
    assert len(queue) == 2
    assert sim.event_counter == 12


def test_queue_fifo_get_1():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(_: SimContext):
        for _ in range(6):
            yield queue.put(item)
        for _ in range(3):
            ret_item = yield queue.get()
            assert ret_item == item

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert ctx.now == 0
    assert len(queue) == 3
    assert len(queue._put_queue) == 0
    assert len(queue._get_queue) == 0


def test_queue_fifo_get_2():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(_: SimContext):
        for _ in range(4):
            queue.put(item)
        for _ in range(5):
            ret_item = yield queue.get()
            assert ret_item == item

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert ctx.now == 0
    assert len(queue) == 0
    assert len(queue._put_queue) == 0
    assert len(queue._get_queue) == 1


def test_queue_fifo_get_3():
    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(3):
            queue.put(item)
        for _ in range(5):
            ret_item = yield queue.get()
            assert ret_item == item
            yield ctx.active_process.timeout(1)

    ctx.add_process(Process(ctx, coro1(ctx)))
    sim.run()
    assert ctx.now == 3
    assert len(queue) == 0
    assert len(queue._put_queue) == 0
    assert len(queue._get_queue) == 1


def test_queue_fifo_1():
    ctx = SimContext()
    sim = Simulator(ctx)

    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(20):
            ret_item = yield queue.get()
            assert ret_item == item
            yield ctx.active_process.timeout(1)

    def coro2(ctx: SimContext):
        for _ in range(10):
            yield queue.put(item)
            yield ctx.active_process.timeout(2)

    ctx.add_process(Process(ctx, coro1(ctx)))
    ctx.add_process(Process(ctx, coro2(ctx)))

    sim.run()
    assert ctx.now == 20
    assert len(queue) == 0
    assert len(queue._put_queue) == 0
    assert len(queue._get_queue) == 1


def test_queue_fifo_2():
    ctx = SimContext()
    sim = Simulator(ctx)

    queue = QueueFIFO(ctx)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(10):
            ret_item = yield queue.get()
            assert ret_item == item
            yield ctx.active_process.timeout(2)

    def coro2(ctx: SimContext):
        for _ in range(20):
            yield queue.put(item)
            yield ctx.active_process.timeout(1)

    ctx.add_process(Process(ctx, coro1(ctx)))
    ctx.add_process(Process(ctx, coro2(ctx)))

    sim.run()
    assert ctx.now == 20
    assert len(queue) == 10
    assert len(queue._put_queue) == 0
    assert len(queue._get_queue) == 0


def test_queue_fifo_3():
    ctx = SimContext()
    sim = Simulator(ctx)

    queue = QueueFIFO(ctx, capacity=2)
    item = "test_item"

    def coro1(ctx: SimContext):
        for _ in range(5):
            ret_item = yield queue.get()
            assert ret_item == item
            yield ctx.active_process.timeout(2)

    def coro2(ctx: SimContext):
        for _ in range(10):
            yield queue.put(item)
            yield ctx.active_process.timeout(1)

    ctx.add_process(Process(ctx, coro1(ctx)))
    ctx.add_process(Process(ctx, coro2(ctx)))

    sim.run()
    assert ctx.now == 10
    assert len(queue) == 2
    assert len(queue._put_queue) == 1
    assert len(queue._get_queue) == 0
