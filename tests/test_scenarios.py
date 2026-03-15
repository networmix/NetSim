"""Tests derived from classic DES scenarios (carwash, gas station, machine shop,
bank renege). These cover real-world interaction patterns between primitives."""

import random

import netsim

# ---------------------------------------------------------------------------
# Renege pattern: request | timeout, then check which fired
# ---------------------------------------------------------------------------


class TestRenegePattern:
    """Customer waits for a resource OR gives up after a patience timeout."""

    def test_customer_served_when_resource_free(self):
        env = netsim.Environment()
        counter = netsim.Resource(env, capacity=1)
        log = []

        def customer(env):
            with counter.request() as req:
                results = yield req | env.timeout(5)
                if req in results:
                    log.append(('served', env.now))
                else:
                    log.append(('reneged', env.now))

        env.process(customer(env))
        env.run()
        assert log == [('served', 0)]

    def test_customer_reneges_when_resource_busy(self):
        env = netsim.Environment()
        counter = netsim.Resource(env, capacity=1)
        log = []

        def holder(env):
            with counter.request() as req:
                yield req
                yield env.timeout(100)

        def customer(env):
            with counter.request() as req:
                results = yield req | env.timeout(3)
                if req in results:
                    log.append(('served', env.now))
                else:
                    log.append(('reneged', env.now))

        env.process(holder(env))
        env.process(customer(env))
        env.run()
        assert log == [('reneged', 3)]

    def test_renege_cancels_request(self):
        """After reneging, the request should be removed from the queue."""
        env = netsim.Environment()
        counter = netsim.Resource(env, capacity=1)

        def holder(env):
            with counter.request() as req:
                yield req
                yield env.timeout(100)

        def reneging_customer(env):
            with counter.request() as req:
                yield req | env.timeout(2)
            # After exiting the with-block, req should be cancelled.

        env.process(holder(env))
        env.process(reneging_customer(env))
        env.run(until=5)
        # Only the holder should be in users; queue should be empty.
        assert counter.count == 1
        assert len(counter.queue) == 0


# ---------------------------------------------------------------------------
# Remaining-work pattern: interrupt, track remaining, resume
# ---------------------------------------------------------------------------


class TestInterruptResume:
    """Process tracks remaining work across interrupts."""

    def test_remaining_work_after_interrupt(self):
        env = netsim.Environment()
        log = []

        def worker(env):
            done_in = 10.0
            while done_in > 0:
                start = env.now
                try:
                    yield env.timeout(done_in)
                    done_in = 0
                except netsim.Interrupt:
                    done_in -= env.now - start
                    log.append(('interrupted', env.now, done_in))
                    yield env.timeout(5)  # repair time
                    log.append(('resumed', env.now))
            log.append(('finished', env.now))

        def breaker(env, proc):
            yield env.timeout(3)
            proc.interrupt()

        proc = env.process(worker(env))
        env.process(breaker(env, proc))
        env.run()

        assert log[0] == ('interrupted', 3, 7.0)
        assert log[1] == ('resumed', 8)
        assert log[2] == ('finished', 15)

    def test_multiple_interrupts_track_remaining(self):
        env = netsim.Environment()
        log = []

        def worker(env):
            done_in = 20.0
            while done_in > 0:
                start = env.now
                try:
                    yield env.timeout(done_in)
                    done_in = 0
                except netsim.Interrupt:
                    done_in -= env.now - start
                    log.append(('break', env.now, round(done_in, 1)))
                    yield env.timeout(2)  # repair
            log.append(('done', env.now))

        def breaker(env, proc):
            yield env.timeout(5)
            proc.interrupt()
            yield env.timeout(12)  # 5 + 2(repair) + 5 more work = t=12
            proc.interrupt()

        proc = env.process(worker(env))
        env.process(breaker(env, proc))
        env.run()

        assert log[0] == ('break', 5, 15.0)
        assert log[1] == ('break', 17, 5.0)
        assert log[2] == ('done', 24)


# ---------------------------------------------------------------------------
# Resource + Container together (gas station pattern)
# ---------------------------------------------------------------------------


class TestResourceAndContainer:
    """Processes use both a Resource (pump) and Container (fuel tank)."""

    def test_pump_and_fuel(self):
        env = netsim.Environment()
        pumps = netsim.Resource(env, capacity=2)
        tank = netsim.Container(env, capacity=100, init=100)
        log = []

        def refuel(env, name, amount):
            with pumps.request() as req:
                yield req
                yield tank.get(amount)
                yield env.timeout(amount / 2)  # 2 liters/sec
                log.append((name, env.now, tank.level))

        env.process(refuel(env, 'A', 30))
        env.process(refuel(env, 'B', 40))
        env.process(refuel(env, 'C', 20))  # waits for pump
        env.run()

        # t=0: A,B get pumps. A draws 30L (tank→70), B draws 40L (tank→30).
        # t=15: A logs (tank=30), then A releases pump, C gets pump, C draws 20L (tank→10).
        # t=20: B logs (tank=10).
        # t=25: C logs (tank=10).
        assert log == [('A', 15.0, 30), ('B', 20.0, 10), ('C', 25.0, 10)]

    def test_container_blocks_until_refilled(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=50, init=10)
        log = []

        def consumer(env):
            yield tank.get(30)  # blocks: only 10 available
            log.append(('consumed', env.now))

        def supplier(env):
            yield env.timeout(5)
            yield tank.put(40)
            log.append(('supplied', env.now))

        env.process(consumer(env))
        env.process(supplier(env))
        env.run()

        assert log[0] == ('supplied', 5)
        assert log[1] == ('consumed', 5)
        assert tank.level == 20  # 10 + 40 - 30 = 20


# ---------------------------------------------------------------------------
# Preemptive repair cycle (machine shop pattern)
# ---------------------------------------------------------------------------


class TestPreemptiveRepairCycle:
    """Machine breaks, preempts repairman's other job, gets repaired."""

    def test_repair_preempts_other_job(self):
        env = netsim.Environment()
        repairman = netsim.PreemptiveResource(env, capacity=1)
        log = []

        def other_job(env):
            while True:
                with repairman.request(priority=2) as req:
                    yield req
                    try:
                        yield env.timeout(30)
                        log.append(('other_done', env.now))
                    except netsim.Interrupt:
                        log.append(('other_preempted', env.now))

        def machine(env):
            yield env.timeout(10)  # break at t=10
            with repairman.request(priority=1) as req:
                yield req
                yield env.timeout(5)  # repair takes 5
                log.append(('repaired', env.now))

        env.process(other_job(env))
        env.process(machine(env))
        env.run(until=50)

        assert log == [('other_preempted', 10), ('repaired', 15), ('other_done', 45)]


# ---------------------------------------------------------------------------
# Carwash: process-as-event while holding resource + continuous spawning
# ---------------------------------------------------------------------------


class TestCarwashPatterns:
    """Patterns from the carwash scenario."""

    def test_yield_process_while_holding_resource(self):
        """A process holds a resource and yields a sub-process inside."""
        env = netsim.Environment()
        machine = netsim.Resource(env, capacity=1)
        log = []

        def wash(env, name):
            yield env.timeout(5)
            log.append(('washed', name, env.now))

        def car(env, name):
            with machine.request() as req:
                yield req
                log.append(('enter', name, env.now))
                yield env.process(wash(env, name))
                log.append(('leave', name, env.now))

        env.process(car(env, 'A'))
        env.process(car(env, 'B'))
        env.run()

        assert log == [
            ('enter', 'A', 0),
            ('washed', 'A', 5),
            ('leave', 'A', 5),
            ('enter', 'B', 5),
            ('washed', 'B', 10),
            ('leave', 'B', 10),
        ]

    def test_continuous_spawning(self):
        """A generator process continuously spawns new processes."""
        env = netsim.Environment()
        arrived = []

        def visitor(env, name):
            arrived.append((name, env.now))
            yield env.timeout(1)

        def source(env):
            for i in range(5):
                yield env.timeout(3)
                env.process(visitor(env, f'V{i}'))

        env.process(source(env))
        env.run()

        assert arrived == [
            ('V0', 3),
            ('V1', 6),
            ('V2', 9),
            ('V3', 12),
            ('V4', 15),
        ]


# ---------------------------------------------------------------------------
# Machine shop: separate failure-generator repeatedly interrupting a worker
# ---------------------------------------------------------------------------


class TestRepeatedFailures:
    """A worker process is repeatedly interrupted by a separate failure process."""

    def test_worker_with_failure_generator(self):
        env = netsim.Environment()
        parts_made = [0]
        repairs = [0]

        def worker(env):
            while True:
                done_in = 10.0
                while done_in > 0:
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except netsim.Interrupt:
                        done_in -= env.now - start
                        yield env.timeout(5)  # repair
                        repairs[0] += 1
                parts_made[0] += 1

        def failures(env, proc):
            while True:
                yield env.timeout(25)
                if proc.is_alive:
                    proc.interrupt()

        proc = env.process(worker(env))
        env.process(failures(env, proc))
        env.run(until=100)

        assert parts_made[0] > 0
        assert repairs[0] > 0


# ---------------------------------------------------------------------------
# Bank: multiple customers, some served, some renege
# ---------------------------------------------------------------------------


class TestMultipleCustomerRenege:
    """Multiple customers with varying patience; some served, some renege."""

    def test_mixed_serve_and_renege(self):
        random.seed(42)
        env = netsim.Environment()
        counter = netsim.Resource(env, capacity=1)
        log = []

        def customer(env, name, patience, service_time):
            with counter.request() as req:
                results = yield req | env.timeout(patience)
                if req in results:
                    yield env.timeout(service_time)
                    log.append(('served', name, env.now))
                else:
                    log.append(('reneged', name, env.now))

        def source(env):
            # Customer 0: patient (patience=100), long service (20)
            env.process(customer(env, 'C0', 100, 20))
            yield env.timeout(1)
            # Customer 1: impatient (patience=2), arrives while C0 is served
            env.process(customer(env, 'C1', 2, 5))
            yield env.timeout(1)
            # Customer 2: patient (patience=50), arrives while C0 is served
            env.process(customer(env, 'C2', 50, 3))

        env.process(source(env))
        env.run()

        served = [e for e in log if e[0] == 'served']
        reneged = [e for e in log if e[0] == 'reneged']

        assert len(served) == 2  # C0 and C2
        assert len(reneged) == 1  # C1
        assert reneged[0][1] == 'C1'
        assert served[0][1] == 'C0'
        assert served[1][1] == 'C2'


# ---------------------------------------------------------------------------
# Gas station: fire-and-forget container put + monitoring process
# ---------------------------------------------------------------------------


class TestContainerMonitoring:
    """A monitoring process watches a container level and triggers refill."""

    def test_monitor_triggers_refill(self):
        env = netsim.Environment()
        tank = netsim.Container(env, capacity=100, init=100)
        log = []

        def consumer(env):
            for _ in range(3):
                yield tank.get(40)
                log.append(('consumed', env.now, tank.level))
                yield env.timeout(10)

        def monitor(env):
            while True:
                if tank.level < 30:
                    log.append(('refill_start', env.now))
                    yield env.timeout(5)  # truck travel time
                    yield tank.put(tank.capacity - tank.level)
                    log.append(('refilled', env.now, tank.level))
                yield env.timeout(1)

        env.process(consumer(env))
        env.process(monitor(env))
        env.run(until=50)  # monitor runs forever; bound the simulation

        assert log == [
            ('consumed', 0, 60),
            ('refill_start', 10),
            ('consumed', 10, 20),
            ('refilled', 15, 100),
            ('consumed', 20, 60),
        ]


# ---------------------------------------------------------------------------
# Sequential resource acquisition (patient → registration → doctor)
# ---------------------------------------------------------------------------


class TestSequentialResources:
    """A job acquires multiple resources in sequence."""

    def test_patient_registration_then_doctor(self):
        env = netsim.Environment()
        desk = netsim.Resource(env, capacity=1)
        doctor = netsim.Resource(env, capacity=1)
        log = []

        def patient(env, name):
            with desk.request() as req:
                yield req
                yield env.timeout(2)  # registration
                log.append(('registered', name, env.now))
            with doctor.request() as req:
                yield req
                yield env.timeout(5)  # consultation
                log.append(('treated', name, env.now))

        env.process(patient(env, 'P1'))
        env.process(patient(env, 'P2'))
        env.run()

        # P1: register 0-2, doctor 2-7
        # P2: register 2-4 (waits for desk), doctor 7-12 (waits for doctor)
        assert log == [
            ('registered', 'P1', 2),
            ('registered', 'P2', 4),
            ('treated', 'P1', 7),
            ('treated', 'P2', 12),
        ]


# ---------------------------------------------------------------------------
# FilterStore: warehouse with typed parts
# ---------------------------------------------------------------------------


class TestFilterStoreScenario:
    """Processes request specific item types from a shared store."""

    def test_workers_request_specific_parts(self):
        env = netsim.Environment()
        warehouse = netsim.FilterStore(env)
        log = []

        def supplier(env):
            for item in ['bolt', 'nut', 'bolt', 'washer', 'nut']:
                yield warehouse.put(item)
                yield env.timeout(1)

        def worker(env, name, needed):
            item = yield warehouse.get(filter=lambda x: x == needed)
            log.append((name, item, env.now))

        env.process(supplier(env))
        env.process(worker(env, 'W1', 'nut'))
        env.process(worker(env, 'W2', 'washer'))
        env.run()

        # W1 wants 'nut' — gets it at t=1 (second item supplied)
        # W2 wants 'washer' — gets it at t=3 (fourth item supplied)
        assert ('W1', 'nut', 1) in log
        assert ('W2', 'washer', 3) in log


# ---------------------------------------------------------------------------
# AllOf as barrier: workers synchronize before next phase
# ---------------------------------------------------------------------------


class TestBarrierSync:
    """N workers must all complete before any proceeds to next phase."""

    def test_barrier_with_processes(self):
        env = netsim.Environment()
        log = []

        def worker(env, name, duration):
            yield env.timeout(duration)
            log.append(('done', name, env.now))

        def coordinator(env):
            # Phase 1: start 3 workers with different durations
            w1 = env.process(worker(env, 'A', 3))
            w2 = env.process(worker(env, 'B', 5))
            w3 = env.process(worker(env, 'C', 1))

            # Wait for all to finish
            yield env.all_of([w1, w2, w3])
            log.append(('barrier', env.now))

            # Phase 2
            yield env.timeout(2)
            log.append(('phase2_done', env.now))

        env.process(coordinator(env))
        env.run()

        # All workers finish by t=5 (slowest is B), barrier at t=5
        assert log == [
            ('done', 'C', 1),
            ('done', 'A', 3),
            ('done', 'B', 5),
            ('barrier', 5),
            ('phase2_done', 7),
        ]


# ---------------------------------------------------------------------------
# Process communication via Store (message passing)
# ---------------------------------------------------------------------------


class TestMessagePassing:
    """Processes communicate by passing messages through a Store."""

    def test_request_response(self):
        env = netsim.Environment()
        requests = netsim.Store(env)
        responses = netsim.Store(env)
        log = []

        def server(env):
            while True:
                msg = yield requests.get()
                yield env.timeout(2)  # processing time
                yield responses.put(f'reply to {msg}')

        def client(env, msg):
            yield requests.put(msg)
            reply = yield responses.get()
            log.append((msg, reply, env.now))

        env.process(server(env))
        env.process(client(env, 'hello'))
        env.process(client(env, 'world'))
        env.run(until=20)

        assert log[0] == ('hello', 'reply to hello', 2)
        assert log[1] == ('world', 'reply to world', 4)


# ---------------------------------------------------------------------------
# Batch accumulation: collect N items, process as batch
# ---------------------------------------------------------------------------


class TestBatchProcessing:
    """Accumulate items in a store, process them in batches."""

    def test_batch_of_three(self):
        env = netsim.Environment()
        buffer = netsim.Store(env)
        log = []

        def producer(env):
            for i in range(7):
                yield buffer.put(i)
                yield env.timeout(1)

        def batch_processor(env, batch_size):
            while True:
                batch = []
                for _ in range(batch_size):
                    item = yield buffer.get()
                    batch.append(item)
                yield env.timeout(3)  # process batch
                log.append((batch, env.now))

        env.process(producer(env))
        env.process(batch_processor(env, 3))
        env.run(until=30)

        # Items produced at t=0,1,2,3,4,5,6
        # Batch 1: items 0,1,2 collected by t=2, processed by t=5
        # Batch 2: items 3,4,5 collected by t=5, processed by t=8
        assert log[0] == ([0, 1, 2], 5)
        assert log[1] == ([3, 4, 5], 8)


# ---------------------------------------------------------------------------
# Simultaneous arrival: deterministic ordering at same time
# ---------------------------------------------------------------------------


class TestDeterministicOrdering:
    """Events at the same time are processed in creation order."""

    def test_same_time_resource_claim(self):
        """Two processes arrive at t=0 for a capacity-1 resource.
        First created process wins."""
        env = netsim.Environment()
        res = netsim.Resource(env, capacity=1)
        log = []

        def worker(env, name):
            with res.request() as req:
                yield req
                log.append(('got', name, env.now))
                yield env.timeout(5)

        env.process(worker(env, 'first'))
        env.process(worker(env, 'second'))
        env.run()

        assert log[0] == ('got', 'first', 0)
        assert log[1] == ('got', 'second', 5)

    def test_timeout_and_event_at_same_time(self):
        """A timeout and a manual event trigger at the same time.
        Order follows scheduling order."""
        env = netsim.Environment()
        log = []

        def waiter(env, evt):
            yield evt
            log.append(('event', env.now))

        def timer(env):
            yield env.timeout(5)
            log.append(('timeout', env.now))

        evt = env.event()
        env.process(waiter(env, evt))
        env.process(timer(env))

        def trigger(env, evt):
            yield env.timeout(5)
            evt.succeed('fired')

        env.process(trigger(env, evt))
        env.run()

        # Both happen at t=5
        assert len(log) == 2
        assert log[0][1] == 5
        assert log[1][1] == 5


# ---------------------------------------------------------------------------
# Cascading stall: failure propagates through pipeline
# ---------------------------------------------------------------------------


class TestCascadingStall:
    """A blockage in one stage stalls the entire pipeline."""

    def test_blocked_stage_stalls_upstream(self):
        env = netsim.Environment()
        s1 = netsim.Store(env, capacity=2)
        s2 = netsim.Store(env, capacity=2)
        log = []

        def producer(env):
            for i in range(6):
                yield s1.put(i)
                log.append(('produced', i, env.now))

        def stage(env, inp, out):
            while True:
                item = yield inp.get()
                yield env.timeout(1)
                yield out.put(item)

        def slow_consumer(env):
            while True:
                item = yield s2.get()
                yield env.timeout(10)  # very slow
                log.append(('consumed', item, env.now))

        env.process(producer(env))
        env.process(stage(env, s1, s2))
        env.process(slow_consumer(env))
        env.run(until=30)

        assert log == [
            ('produced', 0, 0),
            ('produced', 1, 0),
            ('produced', 2, 0),
            ('produced', 3, 1),
            ('produced', 4, 2),
            ('produced', 5, 3),
            ('consumed', 0, 11),
            ('consumed', 1, 21),
        ]


# ---------------------------------------------------------------------------
# Race for any of two resources (first-available pattern)
# ---------------------------------------------------------------------------


class TestFirstAvailableResource:
    """Process waits for whichever of two resources is free first."""

    def test_any_of_two_resources(self):
        env = netsim.Environment()
        res_a = netsim.Resource(env, capacity=1)
        res_b = netsim.Resource(env, capacity=1)
        log = []

        def holder(env, res, name, duration):
            with res.request() as req:
                yield req
                yield env.timeout(duration)

        def flexible_worker(env):
            req_a = res_a.request()
            req_b = res_b.request()
            result = yield req_a | req_b

            if req_a in result:
                log.append(('got_a', env.now))
                yield env.timeout(1)
                res_a.release(req_a)
                req_b.cancel()
            else:
                log.append(('got_b', env.now))
                yield env.timeout(1)
                res_b.release(req_b)
                req_a.cancel()

        # A is busy for 10, B is busy for 3
        env.process(holder(env, res_a, 'holder_a', 10))
        env.process(holder(env, res_b, 'holder_b', 3))
        env.process(flexible_worker(env))
        env.run()

        # Worker should get B first (free at t=3) since A is held until t=10
        assert log == [('got_b', 3)]


# ---------------------------------------------------------------------------
# Preemption chain: three priority levels
# ---------------------------------------------------------------------------


class TestPreemptionChain:
    """Three priority levels: each preempts the one below."""

    def test_three_level_preemption(self):
        env = netsim.Environment()
        res = netsim.PreemptiveResource(env, capacity=1)
        log = []

        def job(env, name, prio, duration):
            with res.request(priority=prio) as req:
                yield req
                try:
                    yield env.timeout(duration)
                    log.append(('done', name, env.now))
                except netsim.Interrupt:
                    log.append(('preempted', name, env.now))

        # Low priority starts at t=0
        env.process(job(env, 'low', 3, 100))

        # Medium arrives at t=2, preempts low
        def spawn_med(env):
            yield env.timeout(2)
            env.process(job(env, 'med', 2, 100))

        # High arrives at t=4, preempts medium
        def spawn_high(env):
            yield env.timeout(4)
            env.process(job(env, 'high', 1, 5))

        env.process(spawn_med(env))
        env.process(spawn_high(env))
        env.run(until=20)

        assert ('preempted', 'low', 2) in log
        assert ('preempted', 'med', 4) in log
        assert ('done', 'high', 9) in log


# ---------------------------------------------------------------------------
# Adaptive rate: consumer adjusts speed based on queue depth
# ---------------------------------------------------------------------------


class TestAdaptiveRate:
    """Consumer slows down when queue is deep, speeds up when shallow."""

    def test_rate_adapts_to_queue_depth(self):
        env = netsim.Environment()
        queue = netsim.Store(env, capacity=100)
        rates = []

        def producer(env):
            for i in range(20):
                yield queue.put(i)
                yield env.timeout(1)

        def adaptive_consumer(env):
            while True:
                yield queue.get()
                depth = len(queue.items)
                delay = 0.5 if depth > 5 else 2.0
                rates.append((env.now, depth, delay))
                yield env.timeout(delay)

        env.process(producer(env))
        env.process(adaptive_consumer(env))
        env.run(until=30)

        assert rates == [
            (0, 0, 2.0),
            (2.0, 1, 2.0),
            (4.0, 2, 2.0),
            (6.0, 3, 2.0),
            (8.0, 4, 2.0),
            (10.0, 5, 2.0),
            (12.0, 6, 0.5),
            (12.5, 5, 2.0),
            (14.5, 6, 0.5),
            (15.0, 6, 0.5),
            (15.5, 5, 2.0),
            (17.5, 6, 0.5),
            (18.0, 6, 0.5),
            (18.5, 5, 2.0),
            (20.5, 5, 2.0),
            (22.5, 4, 2.0),
            (24.5, 3, 2.0),
            (26.5, 2, 2.0),
            (28.5, 1, 2.0),
        ]


# ---------------------------------------------------------------------------
# Conditional spawning: dispatcher routes work based on state
# ---------------------------------------------------------------------------


class TestConditionalDispatch:
    """Dispatcher assigns jobs to workers based on current load."""

    def test_load_based_routing(self):
        env = netsim.Environment()
        q1 = netsim.Store(env, capacity=10)
        q2 = netsim.Store(env, capacity=10)
        log = []

        def worker(env, name, queue):
            while True:
                item = yield queue.get()
                yield env.timeout(3)
                log.append((name, item, env.now))

        def dispatcher(env):
            for i in range(6):
                # Route to shorter queue
                if len(q1.items) <= len(q2.items):
                    yield q1.put(i)
                else:
                    yield q2.put(i)
                yield env.timeout(1)

        env.process(worker(env, 'W1', q1))
        env.process(worker(env, 'W2', q2))
        env.process(dispatcher(env))
        env.run(until=30)

        assert log == [
            ('W1', 0, 3),
            ('W2', 2, 5),
            ('W1', 1, 6),
            ('W2', 4, 8),
            ('W1', 3, 9),
            ('W2', 5, 11),
        ]


# ---------------------------------------------------------------------------
# Dynamic topology: processes as events driving structural changes
# ---------------------------------------------------------------------------


class TestDynamicTopology:
    """Simulation structure changes based on process outcomes."""

    def test_result_driven_spawning(self):
        """Process return value determines what gets spawned next."""
        env = netsim.Environment()
        log = []

        def probe(env, target):
            yield env.timeout(2)
            return target > 3  # True if target is "interesting"

        def deep_analysis(env, target):
            yield env.timeout(5)
            log.append(('analyzed', target, env.now))

        def controller(env):
            for t in [1, 5, 2, 7]:
                interesting = yield env.process(probe(env, t))
                if interesting:
                    yield env.process(deep_analysis(env, t))
                else:
                    log.append(('skipped', t, env.now))

        env.process(controller(env))
        env.run()

        assert log == [
            ('skipped', 1, 2),
            ('analyzed', 5, 9),  # probe=2 + analysis=5 = 7, but starts at t=2
            ('skipped', 2, 11),
            ('analyzed', 7, 18),
        ]

    def test_fan_out_fan_in_with_dynamic_count(self):
        """Spawn variable number of workers, wait for all, use results."""
        env = netsim.Environment()
        log = []

        def worker(env, wid, duration):
            yield env.timeout(duration)
            return wid * 10

        def manager(env):
            for batch_size in [2, 3]:
                workers = [
                    env.process(worker(env, i, i + 1)) for i in range(batch_size)
                ]
                results = yield env.all_of(workers)
                values = [results[w] for w in workers]
                log.append(('batch', batch_size, values, env.now))

        env.process(manager(env))
        env.run()

        assert log[0] == ('batch', 2, [0, 10], 2)  # workers take 1,2 → done at t=2
        assert log[1] == (
            'batch',
            3,
            [0, 10, 20],
            5,
        )  # workers take 1,2,3 → done at t=2+3=5

    def test_dynamic_resource_creation(self):
        """Resources created mid-simulation based on demand."""
        env = netsim.Environment()
        pools = {}
        log = []

        def get_pool(env, name):
            if name not in pools:
                pools[name] = netsim.Resource(env, capacity=1)
                log.append(('created', name, env.now))
            return pools[name]

        def job(env, pool_name, jid):
            pool = get_pool(env, pool_name)
            with pool.request() as req:
                yield req
                yield env.timeout(3)
                log.append(('done', pool_name, jid, env.now))

        def scheduler(env):
            env.process(job(env, 'gpu', 1))
            yield env.timeout(1)
            env.process(job(env, 'cpu', 2))
            yield env.timeout(1)
            env.process(job(env, 'gpu', 3))  # queues behind job 1

        env.process(scheduler(env))
        env.run()

        assert ('created', 'gpu', 0) in log
        assert ('created', 'cpu', 1) in log
        assert ('done', 'gpu', 1, 3) in log
        assert ('done', 'cpu', 2, 4) in log
        assert ('done', 'gpu', 3, 6) in log  # waits for job 1

    def test_process_death_triggers_respawn(self):
        """When a worker dies, a watcher spawns a replacement."""
        env = netsim.Environment()
        log = []
        generation = [0]

        def worker(env, gen):
            yield env.timeout(5)
            log.append(('finished', gen, env.now))
            return gen

        def supervisor(env):
            while generation[0] < 3:
                gen = generation[0]
                proc = env.process(worker(env, gen))
                result = yield proc
                log.append(('respawning', result, env.now))
                generation[0] += 1

        env.process(supervisor(env))
        env.run()

        assert log == [
            ('finished', 0, 5),
            ('respawning', 0, 5),
            ('finished', 1, 10),
            ('respawning', 1, 10),
            ('finished', 2, 15),
            ('respawning', 2, 15),
        ]

    def test_process_watching_multiple_processes(self):
        """A coordinator watches process A, then conditionally starts B or C."""
        env = netsim.Environment()
        log = []

        def fast_task(env):
            yield env.timeout(3)
            return 'fast'

        def slow_task(env):
            yield env.timeout(10)
            return 'slow'

        def fallback(env):
            yield env.timeout(2)
            log.append(('fallback_done', env.now))

        def followup(env):
            yield env.timeout(1)
            log.append(('followup_done', env.now))

        def coordinator(env):
            p = env.process(fast_task(env))
            t = env.timeout(5)  # deadline
            result = yield p | t

            if p in result:
                # Finished before deadline — do followup
                log.append(('on_time', result[p], env.now))
                yield env.process(followup(env))
            else:
                # Missed deadline — fallback
                log.append(('late', env.now))
                yield env.process(fallback(env))

        env.process(coordinator(env))
        env.run()

        # fast_task finishes at t=3, before deadline t=5
        assert log == [
            ('on_time', 'fast', 3),
            ('followup_done', 4),
        ]
