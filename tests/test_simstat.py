from netsim.simcore import Event, SimContext
from netsim.simstat import (
    ProcessStat,
    ProcessStatFrame,
    QueueStat,
    QueueStatFrame,
    ResourceStat,
    ResourceStatFrame,
    Stat,
    StatFrame,
)

import pytest


def test_statframe_1():
    statframe = StatFrame()
    statframe.set_time(timestamp=10, duration=5)

    assert statframe.timestamp == 10
    assert statframe.duration == 5


def test_statframe_2():
    statframe = StatFrame()
    statframe.set_time(timestamp=10, duration=5)

    assert statframe.timestamp == 10
    assert statframe.duration == 5

    statframe.reset_stat()

    assert statframe.timestamp == 10
    assert statframe.duration == 0


def test_statframe_3():
    statframe = StatFrame()
    statframe.set_time(timestamp=10, duration=5)

    assert statframe.todict() == {
        "duration": 5,
        "last_state_change_timestamp": 0,
        "timestamp": 10,
    }


def test_stat_advance_time_1():
    ctx = SimContext()
    stat = Stat(ctx)

    assert stat.cur_interval_duration == 0
    assert stat.start_interval_timestamp == 0

    assert stat.cur_stat_frame.timestamp == 0
    assert stat.cur_stat_frame.duration == 0

    ctx.advance_simtime(10)
    stat.advance_time()

    assert stat.cur_timestamp == 10
    assert stat.prev_timestamp == 0
    assert stat.cur_interval_duration == 10
    assert stat.start_interval_timestamp == 0

    assert stat.cur_stat_frame.timestamp == 10
    assert stat.cur_stat_frame.duration == 10


def test_stat_reset_1():
    ctx = SimContext()
    stat = Stat(ctx)

    stat.cur_timestamp = 10
    stat.prev_timestamp = 0
    stat.cur_interval_duration = 10
    stat.start_interval_timestamp = 0

    stat.cur_stat_frame.timestamp = 10
    stat.cur_stat_frame.duration = 10

    stat.reset_stat()

    assert stat.cur_timestamp == 10
    assert stat.prev_timestamp == 0
    assert stat.cur_interval_duration == 0
    assert stat.start_interval_timestamp == 10

    assert stat.cur_stat_frame.timestamp == 10
    assert stat.cur_stat_frame.duration == 0


def test_stat_reset_2():
    ctx = SimContext()
    stat = Stat(ctx)

    assert stat.cur_stat_frame.timestamp == 0
    assert stat.cur_interval_duration == 0
    assert stat.start_interval_timestamp == 0

    ctx.advance_simtime(10)
    stat.advance_time()
    stat.reset_stat()

    assert stat.cur_timestamp == 10
    assert stat.prev_timestamp == 0
    assert stat.cur_interval_duration == 0
    assert stat.start_interval_timestamp == 10

    assert stat.cur_stat_frame.timestamp == 10


def test_statframe_process_reset_1():
    proc_statframe = ProcessStatFrame()

    proc_statframe.timestamp += 100
    proc_statframe.total_event_exec_count += 10
    proc_statframe.total_event_gen_count += 10

    assert proc_statframe.total_event_exec_count == 10
    assert proc_statframe.total_event_gen_count == 10

    proc_statframe.reset_stat()

    assert proc_statframe.total_event_exec_count == 0
    assert proc_statframe.total_event_gen_count == 0
    assert proc_statframe.timestamp == 100


def test_statframe_process_1():
    proc_statframe = ProcessStatFrame()

    for _ in range(10):
        proc_statframe.event_generated()
    assert proc_statframe.total_event_gen_count == 10

    for _ in range(10):
        proc_statframe.event_exec()
    assert proc_statframe.total_event_exec_count == 10

    proc_statframe.set_time(100, 100)
    proc_statframe.reset_stat()

    assert proc_statframe.total_event_exec_count == 0
    assert proc_statframe.total_event_gen_count == 0
    assert proc_statframe.timestamp == 100
    assert proc_statframe.duration == 0


def test_statframe_process_2():
    proc_statframe = ProcessStatFrame()

    for _ in range(10):
        proc_statframe.event_generated()
    assert proc_statframe.total_event_gen_count == 10

    for _ in range(10):
        proc_statframe.event_exec()
    assert proc_statframe.total_event_exec_count == 10

    proc_statframe.set_time(100, 100)
    proc_statframe.update_stat()

    assert proc_statframe.timestamp == 100
    assert proc_statframe.duration == 100

    assert proc_statframe.total_event_exec_count == 10
    assert proc_statframe.total_event_gen_count == 10

    assert proc_statframe.avg_event_exec_rate == 0.1
    assert proc_statframe.avg_event_gen_rate == 0.1


def test_statframe_resource_1():
    res_statframe = ResourceStatFrame()

    for _ in range(10):
        res_statframe.put_requested()
    assert res_statframe.total_put_requested_count == 10

    for _ in range(10):
        res_statframe.put_processed()
    assert res_statframe.total_put_processed_count == 10

    for _ in range(2):
        res_statframe.get_requested()
    assert res_statframe.total_get_requested_count == 2

    for _ in range(2):
        res_statframe.get_processed()
    assert res_statframe.total_get_requested_count == 2


def test_statframe_queue_1():
    queue_statframe = QueueStatFrame()

    for _ in range(10):
        queue_statframe.put_requested()
    assert queue_statframe.total_put_requested_count == 10

    for _ in range(10):
        queue_statframe.put_processed()
    assert queue_statframe.total_put_processed_count == 10
    assert queue_statframe.cur_queue_len == 10

    for _ in range(2):
        queue_statframe.get_requested()
    assert queue_statframe.total_get_requested_count == 2
    assert queue_statframe.cur_queue_len == 10

    for _ in range(2):
        queue_statframe.get_processed()
    assert queue_statframe.total_get_requested_count == 2
    assert queue_statframe.cur_queue_len == 8


def test_process_stat_1():
    ctx = SimContext()
    event = Event(ctx)

    procstat = ProcessStat(ctx)

    for _ in range(5):
        procstat.event_generated(event)
    assert procstat.cur_stat_frame.total_event_gen_count == 5

    for _ in range(3):
        procstat.event_exec(event)
    assert procstat.cur_stat_frame.total_event_exec_count == 3

    ctx.advance_simtime(10)
    procstat.advance_time()

    assert procstat.cur_timestamp == 10
    assert procstat.cur_stat_frame.timestamp == 10
    assert procstat.prev_stat_frame.timestamp == 0
    assert procstat.cur_stat_frame.total_event_gen_count == 5
    assert procstat.cur_stat_frame.total_event_exec_count == 3


def test_process_stat_2():
    ctx = SimContext()
    event = Event(ctx)

    procstat = ProcessStat(ctx)

    for _ in range(5):
        procstat.event_generated(event)
    assert procstat.cur_stat_frame.total_event_gen_count == 5

    for _ in range(5):
        procstat.event_exec(event)
    assert procstat.cur_stat_frame.total_event_exec_count == 5

    ctx.advance_simtime(10)
    procstat.advance_time()

    for _ in range(5):
        procstat.event_generated(event)
    assert procstat.cur_stat_frame.total_event_gen_count == 10
    assert procstat.prev_stat_frame.total_event_gen_count == 5

    for _ in range(5):
        procstat.event_exec(event)
    assert procstat.cur_stat_frame.total_event_exec_count == 10
    assert procstat.prev_stat_frame.total_event_exec_count == 5

    ctx.advance_simtime(20)
    procstat.advance_time()

    assert procstat.cur_timestamp == 20
    assert procstat.last_state_change_timestamp == 10
    assert procstat.cur_stat_frame.timestamp == 20
    assert procstat.prev_stat_frame.timestamp == 10

    assert procstat.cur_stat_frame.total_event_gen_count == 10
    assert procstat.cur_stat_frame.total_event_exec_count == 10


def test_process_stat_3():
    ctx = SimContext()
    event = Event(ctx)

    procstat = ProcessStat(ctx)

    for _ in range(5):
        procstat.event_generated(event)

    for _ in range(5):
        procstat.event_exec(event)

    ctx.advance_simtime(10)
    procstat.advance_time()

    for _ in range(5):
        procstat.event_generated(event)

    for _ in range(5):
        procstat.event_exec(event)

    ctx.advance_simtime(20)
    procstat.advance_time()

    assert procstat.todict() == {
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
            "avg_event_exec_rate": 1.0,
            "avg_event_gen_rate": 1.0,
            "duration": 10,
            "last_state_change_timestamp": 10,
            "timestamp": 10,
            "total_event_exec_count": 10,
            "total_event_gen_count": 10,
        },
        "prev_timestamp": 10,
        "start_interval_timestamp": 0,
    }


def test_process_stat_4():
    ctx = SimContext()
    event = Event(ctx)

    procstat = ProcessStat(ctx)

    for _ in range(5):
        procstat.event_generated(event)

    for _ in range(5):
        procstat.event_exec(event)

    ctx.advance_simtime(10)
    procstat.advance_time()
    procstat.reset_stat()

    for _ in range(5):
        procstat.event_generated(event)

    for _ in range(5):
        procstat.event_exec(event)

    ctx.advance_simtime(20)
    procstat.advance_time()

    assert procstat.todict() == {
        "cur_interval_duration": 10,
        "cur_stat_frame": {
            "avg_event_exec_rate": 0.5,
            "avg_event_gen_rate": 0.5,
            "duration": 10,
            "last_state_change_timestamp": 10,
            "timestamp": 20,
            "total_event_exec_count": 5,
            "total_event_gen_count": 5,
        },
        "cur_timestamp": 20,
        "last_state_change_timestamp": 10,
        "prev_stat_frame": {
            "avg_event_exec_rate": 0,
            "avg_event_gen_rate": 0,
            "duration": 0,
            "last_state_change_timestamp": 10,
            "timestamp": 10,
            "total_event_exec_count": 5,
            "total_event_gen_count": 5,
        },
        "prev_timestamp": 10,
        "start_interval_timestamp": 10,
    }


def test_resource_stat_1():
    ctx = SimContext()
    event = Event(ctx)

    resstat = ResourceStat(ctx)

    for _ in range(5):
        resstat.get_requested(event)
    assert resstat.cur_stat_frame.total_get_requested_count == 5

    for _ in range(3):
        resstat.get_processed(event)
    assert resstat.cur_stat_frame.total_get_processed_count == 3

    for _ in range(5):
        resstat.put_requested(event)
    assert resstat.cur_stat_frame.total_put_requested_count == 5

    for _ in range(3):
        resstat.put_processed(event)
    assert resstat.cur_stat_frame.total_put_processed_count == 3

    ctx.advance_simtime(10)
    resstat.advance_time()

    assert resstat.cur_timestamp == 10
    assert resstat.cur_stat_frame.timestamp == 10
    assert resstat.prev_stat_frame.timestamp == 0
    assert resstat.cur_stat_frame.total_get_requested_count == 5
    assert resstat.cur_stat_frame.total_get_processed_count == 3
    assert resstat.cur_stat_frame.total_put_requested_count == 5
    assert resstat.cur_stat_frame.total_put_processed_count == 3


def test_resource_stat_2():
    ctx = SimContext()
    event = Event(ctx)

    resstat = ResourceStat(ctx)

    for _ in range(5):
        resstat.get_requested(event)
    assert resstat.cur_stat_frame.total_get_requested_count == 5

    for _ in range(5):
        resstat.get_processed(event)
    assert resstat.cur_stat_frame.total_get_processed_count == 5

    for _ in range(10):
        resstat.put_requested(event)
    assert resstat.cur_stat_frame.total_put_requested_count == 10

    for _ in range(10):
        resstat.put_processed(event)
    assert resstat.cur_stat_frame.total_put_processed_count == 10

    ctx.advance_simtime(10)
    resstat.advance_time()

    for _ in range(5):
        resstat.get_requested(event)
    assert resstat.cur_stat_frame.total_get_requested_count == 10

    for _ in range(5):
        resstat.get_processed(event)
    assert resstat.cur_stat_frame.total_get_processed_count == 10

    for _ in range(10):
        resstat.put_requested(event)
    assert resstat.cur_stat_frame.total_put_requested_count == 20

    for _ in range(10):
        resstat.put_processed(event)
    assert resstat.cur_stat_frame.total_put_processed_count == 20

    ctx.advance_simtime(20)
    resstat.advance_time()

    assert resstat.cur_stat_frame.todict() == {
        "avg_get_processed_rate": 0.5,
        "avg_get_requested_rate": 0.5,
        "avg_put_processed_rate": 1.0,
        "avg_put_requested_rate": 1.0,
        "duration": 20,
        "last_state_change_timestamp": 10,
        "timestamp": 20,
        "total_get_processed_count": 10,
        "total_get_requested_count": 10,
        "total_put_processed_count": 20,
        "total_put_requested_count": 20,
    }


def test_queue_stat_1():
    ctx = SimContext()
    event = Event(ctx)

    qstat = QueueStat(ctx)

    for _ in range(5):
        qstat.get_requested(event)
    assert qstat.cur_stat_frame.total_get_requested_count == 5
    assert qstat.cur_stat_frame.cur_queue_len == 0

    with pytest.raises(RuntimeError) as e:
        qstat.get_processed(event)
        assert "cur_queue_len can't become negative" in str(e.value)


def test_queue_stat_2():
    ctx = SimContext()
    event = Event(ctx)

    qstat = QueueStat(ctx)

    for _ in range(5):
        qstat.get_requested(event)
    assert qstat.cur_stat_frame.total_get_requested_count == 5
    assert qstat.cur_stat_frame.cur_queue_len == 0

    for _ in range(10):
        qstat.put_requested(event)
    assert qstat.cur_stat_frame.total_put_requested_count == 10
    assert qstat.cur_stat_frame.cur_queue_len == 0

    for _ in range(10):
        qstat.put_processed(event)
    assert qstat.cur_stat_frame.total_put_processed_count == 10
    assert qstat.cur_stat_frame.cur_queue_len == 10

    ctx.advance_simtime(10)
    qstat.advance_time()

    for _ in range(5):
        qstat.get_requested(event)
    assert qstat.cur_stat_frame.total_get_requested_count == 10
    assert qstat.cur_stat_frame.cur_queue_len == 10
    assert qstat.cur_stat_frame.integral_queue_sum == 100
    assert qstat.cur_stat_frame.avg_queue_len == 10

    for _ in range(5):
        qstat.get_processed(event)
    assert qstat.cur_stat_frame.total_get_processed_count == 5
    assert qstat.cur_stat_frame.cur_queue_len == 5

    for _ in range(10):
        qstat.put_requested(event)
    assert qstat.cur_stat_frame.total_put_requested_count == 20
    assert qstat.cur_stat_frame.cur_queue_len == 5

    for _ in range(10):
        qstat.put_processed(event)
    assert qstat.cur_stat_frame.total_put_processed_count == 20
    assert qstat.cur_stat_frame.cur_queue_len == 15

    ctx.advance_simtime(20)
    qstat.advance_time()

    assert qstat.cur_stat_frame.integral_queue_sum == 250
    assert qstat.cur_stat_frame.avg_queue_len == 12.5

    assert qstat.prev_stat_frame.todict() == {
        "avg_get_processed_rate": 0.5,
        "avg_get_requested_rate": 1.0,
        "avg_put_processed_rate": 2.0,
        "avg_put_requested_rate": 2.0,
        "avg_queue_len": 10.0,
        "cur_queue_len": 15,
        "duration": 10,
        "integral_queue_sum": 100,
        "last_state_change_timestamp": 10,
        "max_queue_len": 15,
        "timestamp": 10,
        "total_get_processed_count": 5,
        "total_get_requested_count": 10,
        "total_put_processed_count": 20,
        "total_put_requested_count": 20,
    }

    assert qstat.cur_stat_frame.todict() == {
        "avg_get_processed_rate": 0.25,
        "avg_get_requested_rate": 0.5,
        "avg_put_processed_rate": 1.0,
        "avg_put_requested_rate": 1.0,
        "avg_queue_len": 12.5,
        "cur_queue_len": 15,
        "duration": 20,
        "integral_queue_sum": 250,
        "last_state_change_timestamp": 10,
        "max_queue_len": 15,
        "timestamp": 20,
        "total_get_processed_count": 5,
        "total_get_requested_count": 10,
        "total_put_processed_count": 20,
        "total_put_requested_count": 20,
    }

    qstat.reset_stat()
    assert qstat.cur_stat_frame.todict() == {
        "avg_get_processed_rate": 0,
        "avg_get_requested_rate": 0,
        "avg_put_processed_rate": 0,
        "avg_put_requested_rate": 0,
        "avg_queue_len": 0,
        "cur_queue_len": 15,
        "duration": 0,
        "integral_queue_sum": 0,
        "last_state_change_timestamp": 10,
        "max_queue_len": 0,
        "timestamp": 20,
        "total_get_processed_count": 0,
        "total_get_requested_count": 0,
        "total_put_processed_count": 0,
        "total_put_requested_count": 0,
    }

    ctx.advance_simtime(30)
    qstat.advance_time()

    assert qstat.cur_stat_frame.todict() == {
        "avg_get_processed_rate": 0.0,
        "avg_get_requested_rate": 0.0,
        "avg_put_processed_rate": 0.0,
        "avg_put_requested_rate": 0.0,
        "avg_queue_len": 15.0,
        "cur_queue_len": 15,
        "duration": 10,
        "integral_queue_sum": 150,
        "last_state_change_timestamp": 10,
        "max_queue_len": 15,
        "timestamp": 30,
        "total_get_processed_count": 0,
        "total_get_requested_count": 0,
        "total_put_processed_count": 0,
        "total_put_requested_count": 0,
    }
