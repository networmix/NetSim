import pytest
from netsim.core import (
    SimContext,
    Resource,
    QueueFIFO,
    Process,
    EventStatus,
)


def test_resource_abstract():
    """Confirm the abstract Resource class cannot be directly instantiated."""
    ctx = SimContext()
    with pytest.raises(TypeError):
        Resource(ctx)  # type: ignore


def test_queue_fifo_put_get_coverage():
    """
    Produce 4 items each with a 1-unit delay and consume them also with a 1-unit delay.
    Typically ends around time=5, showing concurrency overlap.
    """
    from netsim.core import Simulator

    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx, capacity=2)

    def producer(_ctx):
        for i in range(4):
            yield ctx.active_process.timeout(1)
            yield queue.put(f"item_{i}")

    def consumer(_ctx):
        for _ in range(4):
            val = yield queue.get()
            yield ctx.active_process.timeout(1)
            assert "item_" in val

    Process(ctx, producer(ctx))
    Process(ctx, consumer(ctx))

    sim.run()
    # The queue should be empty
    assert len(queue) == 0
    # The final time is likely 5 under concurrency
    assert ctx.now == 5
    # Ensure at least 8 events (4 put + 4 get + timeouts, some merges may reduce total)
    assert sim.event_counter >= 8


def test_putqueuefifo_basic_flow():
    """Check a simple PutQueueFIFO flow with an active process."""
    from netsim.core import PutQueueFIFO

    ctx = SimContext()
    queue = QueueFIFO(ctx)

    def p_coro(_ctx):
        yield queue.put("hello")

    proc = Process(ctx, p_coro(ctx))
    proc.start()

    # Advance the simulation
    e = ctx.get_event()  # Should be PutQueueFIFO
    e.run()
    assert len(queue) == 1
    assert e.status == EventStatus.PROCESSED


def test_getqueuefifo_basic_flow():
    """Confirm a GetQueueFIFO creation and basic flow: produce then consume one item."""
    from netsim.core import GetQueueFIFO

    ctx = SimContext()
    queue = QueueFIFO(ctx)

    def prod(_ctx):
        yield queue.put("hello")

    def cons(_ctx):
        val = yield queue.get()
        assert val == "hello"

    p_prod = Process(ctx, prod(ctx))
    p_cons = Process(ctx, cons(ctx))

    p_prod.start()
    p_cons.start()

    # Run the put first
    e1 = ctx.get_event()
    e1.run()
    assert len(queue) == 1

    # The resource triggers the get
    e2 = ctx.get_event()
    e2.run()
    assert len(queue) == 0


def test_queuefifo_in_timeout():
    """
    If a process is in_timeout, we do not allow immediate put admission.
    Once the process resumes after the timeout, the put is executed.
    """
    from netsim.core import Simulator

    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx, capacity=2)

    def p_coro(_ctx):
        yield ctx.active_process.timeout(5)
        yield queue.put("test_item")

    Process(ctx, p_coro(ctx))
    sim.run()

    # The process sees time=5, then does the put
    # Final time is 5, queue has 1 item
    assert ctx.now == 5
    assert len(queue) == 1
    # At least 2 events (the timeout, the put)
    assert sim.event_counter > 1


def test_queue_fifo_internal_queues():
    """
    With capacity=1, the second put must wait for a get, ensuring concurrency
    can handle blocking on capacity.
    """
    from netsim.core import Simulator

    ctx = SimContext()
    sim = Simulator(ctx)
    queue = QueueFIFO(ctx, capacity=1)

    def producer_coro(_ctx):
        yield queue.put("item1")
        yield queue.put("item2")

    def consumer_coro(_ctx):
        val1 = yield queue.get()
        val2 = yield queue.get()
        assert val1 == "item1"
        assert val2 == "item2"

    Process(ctx, producer_coro(ctx))
    Process(ctx, consumer_coro(ctx))
    sim.run()

    # Both items consumed -> queue empty
    assert len(queue) == 0
    assert len(queue._put_queue) == 0
    assert len(queue._get_queue) == 0


def test_queue_fifo_repr():
    """Quick verification that QueueFIFO's __repr__ contains expected content."""
    ctx = SimContext()
    queue = QueueFIFO(ctx, capacity=3, name="MyQueue")
    rep = repr(queue)
    assert "MyQueue" in rep
    assert "(res_id=0, capacity=3)" in rep
