import pytest
from netsim.core import SimContext, Simulator, Process, QueueFIFO, ProcessStatus
from netsim.stat import SimStat


def test_integration_advanced_stats():
    """
    Integration test with multiple producers and consumers sharing a single FIFO queue.
    - 2 producers each produce 5 items total => 10 items enqueued.
    - 2 consumers each consume 3 items total => 6 items dequeued.
    - Run until time=10 to force partial completion.
    - Verify leftover items, and confirm final stats in sim.stat for processes and the resource.
    """

    ctx = SimContext()
    # stat_interval=None => final stats are collected automatically at the end
    sim = Simulator(ctx, stat_interval=None)

    queue = QueueFIFO(ctx, capacity=None, name="SharedQueue")

    def producer_coro(_ctx, pid):
        """Each producer enqueues 5 items, one per time unit."""
        for i in range(5):
            yield ctx.active_process.timeout(1)
            yield queue.put(f"producer{pid}_item_{i}")

    def consumer_coro(_ctx, cid):
        """Each consumer dequeues 3 items, with a small timeout between gets."""
        for _ in range(3):
            yield ctx.active_process.timeout(0.5)
            _ = yield queue.get()

    # Create 2 producers
    p_prod1 = Process(ctx, producer_coro(ctx, pid=1), name="Producer1")
    p_prod2 = Process(ctx, producer_coro(ctx, pid=2), name="Producer2")

    # Create 2 consumers
    p_cons1 = Process(ctx, consumer_coro(ctx, cid=1), name="Consumer1")
    p_cons2 = Process(ctx, consumer_coro(ctx, cid=2), name="Consumer2")

    # All processes are automatically registered in the context; run them
    sim.run(until_time=10)

    # 10 items produced total; 6 items consumed => 4 items remain
    assert len(queue) == 4
    # Verify that the simulation time is exactly 10
    assert ctx.now == 10
    # Processes should be STOPPED (they yield only a finite number of events)
    assert p_prod1.status == ProcessStatus.STOPPED
    assert p_prod2.status == ProcessStatus.STOPPED
    assert p_cons1.status == ProcessStatus.STOPPED
    assert p_cons2.status == ProcessStatus.STOPPED

    # ---- Check final aggregated statistics ----
    # sim.stat is a SimStat that holds process_stat_samples and resource_stat_samples
    sim_stat: SimStat = sim.stat

    # 1) Check resource stats (for our queue) in the time interval (0,10)
    assert (
        queue.name in sim_stat.resource_stat_samples
    ), "Queue stats not found in SimStat!"
    intervals = list(sim_stat.resource_stat_samples[queue.name].keys())
    assert (
        len(intervals) == 1
    ), "Expected exactly one interval for final stats collection."
    (start_t, end_t) = intervals[0]
    assert (start_t, end_t) == (0, 10)

    queue_stats = sim_stat.resource_stat_samples[queue.name][(0, 10)]
    # At minimum, we expect 10 put requests and 6 get requests
    assert queue_stats.total_put_requested_count == 10
    assert queue_stats.total_put_processed_count == 10
    assert queue_stats.total_get_requested_count == 6
    assert queue_stats.total_get_processed_count == 6

    # 2) Check process stats. Each producer generated 5 Put events and 5 Timeout events, so total_event_gen_count=10
    #    and each consumer generated 3 Get events and 3 Timeout events, so total_event_gen_count=6.
    for proc in (p_prod1, p_prod2, p_cons1, p_cons2):
        proc_name = proc.name
        assert proc_name in sim_stat.process_stat_samples, f"{proc_name} stats missing!"
        intervals = list(sim_stat.process_stat_samples[proc_name].keys())
        # Should have exactly one final interval (0,10)
        assert len(intervals) == 1
        (start_t, end_t) = intervals[0]
        assert (start_t, end_t) == (0, 10)
        proc_stats = sim_stat.process_stat_samples[proc_name][(0, 10)]

        if "Producer" in proc_name:
            # Each producer should have generated 10 events
            assert proc_stats.total_event_gen_count == 10
        else:
            # Each consumer should have generated 6 events
            assert proc_stats.total_event_gen_count == 6
