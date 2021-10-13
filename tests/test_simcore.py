from netsim.simcore import SimContext, Event, Simulator, Process


def test_simcontext_1():
    ctx = SimContext()
    event = Event(ctx, 1, lambda ctx: "TestEvent")

    ctx.add_event(event)
    assert ctx.get_event() == event


def test_simcontext_pq_1():
    ctx = SimContext()
    events = [
        Event(ctx, 1, lambda ctx: "TestEvent1"),
        Event(ctx, 2, lambda ctx: "TestEvent2"),
        Event(ctx, 2, lambda ctx: "TestEvent3", priority=1),
    ]

    for event in events:
        ctx.add_event(event)

    assert ctx.get_event().time == 1
    assert (itm := ctx.get_event()).time == 2 and itm.priority == 1
    assert (itm := ctx.get_event()).time == 2 and itm.priority == 10


def test_simcontext_stop_1():
    ctx = SimContext()

    assert not ctx.stopped
    ctx.stop()
    assert ctx.stopped


def test_simulator_1():
    assert Simulator()


def test_simulator_2():
    sim = Simulator()
    sim.run()


def test_simulator_3():
    ctx = SimContext()
    sim = Simulator(ctx)

    events = [
        Event(ctx, 1, lambda ctx: "TestEvent1"),
        Event(ctx, 2, lambda ctx: "TestEvent2"),
        Event(ctx, 2, lambda ctx: "TestEvent3", priority=1),
    ]

    for event in events:
        ctx.add_event(event)

    sim.run()


def test_simulator_4():
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


def test_simulator_5():
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


def test_simulator_6():
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


def test_simulator_7():
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
