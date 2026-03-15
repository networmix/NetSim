"""Advanced scenario tests exercising complex multi-primitive interactions,
emergent behavior, and real-world modeling patterns."""

import netsim

# ---------------------------------------------------------------------------
# Circuit breaker: disable resource after too many failures
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    """Monitor failure rate and disable/re-enable a service."""

    def test_circuit_breaker_trips_and_recovers(self):
        env = netsim.Environment()
        service = netsim.Resource(env, capacity=1)
        log = []
        circuit_open = [False]

        def request_handler(env, rid):
            if circuit_open[0]:
                log.append(('rejected', rid, env.now))
                return
            with service.request() as req:
                yield req
                yield env.timeout(1)
                # Simulate: requests 3,4,5 fail
                if 3 <= rid <= 5:
                    log.append(('failed', rid, env.now))
                else:
                    log.append(('ok', rid, env.now))

        def breaker_monitor(env):
            """Open circuit after 3 failures in window, close after cooldown."""
            while True:
                yield env.timeout(1)
                recent_fails = sum(
                    1 for e in log if e[0] == 'failed' and e[2] > env.now - 10
                )
                if not circuit_open[0] and recent_fails >= 3:
                    circuit_open[0] = True
                    log.append(('circuit_opened', env.now))
                    yield env.timeout(10)  # cooldown
                    circuit_open[0] = False
                    log.append(('circuit_closed', env.now))

        def traffic(env):
            for i in range(10):
                env.process(request_handler(env, i))
                yield env.timeout(2)

        env.process(traffic(env))
        env.process(breaker_monitor(env))
        env.run(until=40)

        assert log == [
            ('ok', 0, 1),
            ('ok', 1, 3),
            ('ok', 2, 5),
            ('failed', 3, 7),
            ('failed', 4, 9),
            ('failed', 5, 11),
            ('circuit_opened', 12),
            ('ok', 6, 13),
            ('rejected', 7, 14),
            ('rejected', 8, 16),
            ('rejected', 9, 18),
            ('circuit_closed', 22),
        ]


# ---------------------------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------------------------


class TestRetryWithBackoff:
    """Process retries a failing operation with increasing delays."""

    def test_retry_succeeds_on_third_attempt(self):
        env = netsim.Environment()
        log = []
        attempt_count = [0]

        def unreliable_service(env):
            attempt_count[0] += 1
            yield env.timeout(1)
            if attempt_count[0] < 3:
                raise ValueError(f'attempt {attempt_count[0]} failed')
            return 'success'

        def client(env):
            backoff = 1
            for attempt in range(5):
                try:
                    result = yield env.process(unreliable_service(env))
                    log.append(('success', result, env.now))
                    return
                except ValueError:
                    log.append(('retry', attempt + 1, env.now))
                    yield env.timeout(backoff)
                    backoff *= 2
            log.append(('gave_up', env.now))

        env.process(client(env))
        env.run()

        assert log == [
            ('retry', 1, 1),  # attempt 1 fails at t=1, wait 1
            ('retry', 2, 3),  # attempt 2 fails at t=3, wait 2
            ('success', 'success', 6),  # attempt 3 succeeds at t=6
        ]

    def test_retry_gives_up(self):
        env = netsim.Environment()
        log = []

        def always_fails(env):
            yield env.timeout(1)
            raise ValueError('nope')

        def client(env):
            for attempt in range(3):
                try:
                    yield env.process(always_fails(env))
                    return
                except ValueError:
                    log.append(('retry', attempt + 1, env.now))
                    yield env.timeout(1)
            log.append(('gave_up', env.now))

        env.process(client(env))
        env.run()

        assert log[-1] == ('gave_up', 6)


# ---------------------------------------------------------------------------
# Multi-phase job with partial failure and cleanup
# ---------------------------------------------------------------------------


class TestMultiPhaseJob:
    """Job has multiple phases; failure mid-way triggers cleanup."""

    def test_failure_triggers_cleanup(self):
        env = netsim.Environment()
        res_a = netsim.Resource(env, capacity=1)
        res_b = netsim.Resource(env, capacity=1)
        log = []

        def job(env, fail_at_phase):
            # Phase 1: acquire resource A
            req_a = res_a.request()
            yield req_a
            log.append(('acquired_a', env.now))
            yield env.timeout(2)

            if fail_at_phase == 2:
                log.append(('failed_phase2', env.now))
                res_a.release(req_a)
                log.append(('released_a', env.now))
                return

            # Phase 2: acquire resource B while holding A
            req_b = res_b.request()
            yield req_b
            log.append(('acquired_b', env.now))
            yield env.timeout(3)

            # Phase 3: release both
            res_b.release(req_b)
            res_a.release(req_a)
            log.append(('completed', env.now))

        env.process(job(env, fail_at_phase=2))
        env.run()

        assert log == [
            ('acquired_a', 0),
            ('failed_phase2', 2),
            ('released_a', 2),
        ]
        assert res_a.count == 0
        assert res_b.count == 0

    def test_successful_multi_phase(self):
        env = netsim.Environment()
        res_a = netsim.Resource(env, capacity=1)
        res_b = netsim.Resource(env, capacity=1)
        log = []

        def job(env, fail_at_phase):
            req_a = res_a.request()
            yield req_a
            log.append(('acquired_a', env.now))
            yield env.timeout(2)

            if fail_at_phase == 2:
                res_a.release(req_a)
                return

            req_b = res_b.request()
            yield req_b
            log.append(('acquired_b', env.now))
            yield env.timeout(3)

            res_b.release(req_b)
            res_a.release(req_a)
            log.append(('completed', env.now))

        env.process(job(env, fail_at_phase=None))
        env.run()

        assert log == [
            ('acquired_a', 0),
            ('acquired_b', 2),
            ('completed', 5),
        ]
        assert res_a.count == 0
        assert res_b.count == 0


# ---------------------------------------------------------------------------
# Work stealing: idle worker takes from busy worker's queue
# ---------------------------------------------------------------------------


class TestWorkStealing:
    """Idle workers steal work from other workers' queues."""

    def test_steal_balances_load(self):
        env = netsim.Environment()
        q1 = netsim.Store(env)
        q2 = netsim.Store(env)
        log = []

        def worker(env, name, own_queue, other_queue):
            while True:
                # Try own queue first, steal from other if empty
                own_req = own_queue.get()
                other_req = other_queue.get()
                result = yield own_req | other_req

                if own_req in result:
                    item = result[own_req]
                    other_req.cancel()
                    source = 'own'
                else:
                    item = result[other_req]
                    own_req.cancel()
                    source = 'stolen'

                yield env.timeout(2)
                log.append((name, item, source, env.now))

        def loader(env):
            # Load all work into q1, nothing into q2
            for i in range(4):
                yield q1.put(f'job{i}')

        env.process(loader(env))
        env.process(worker(env, 'W1', q1, q2))
        env.process(worker(env, 'W2', q2, q1))
        env.run(until=15)

        assert log == [
            ('W1', 'job0', 'own', 2),
            ('W2', 'job1', 'stolen', 2),
            ('W1', 'job2', 'own', 4),
            ('W2', 'job3', 'stolen', 4),
        ]


# ---------------------------------------------------------------------------
# Producer with backpressure: checks downstream before producing
# ---------------------------------------------------------------------------


class TestBackpressure:
    """Producer slows down when downstream buffer is nearly full."""

    def test_producer_respects_buffer_depth(self):
        env = netsim.Environment()
        buffer = netsim.Store(env, capacity=5)
        log = []

        def producer(env):
            for i in range(10):
                # Backpressure: wait if buffer > 3
                while len(buffer.items) >= 3:
                    log.append(('backpressure', i, env.now))
                    yield env.timeout(2)
                yield buffer.put(i)
                log.append(('produced', i, env.now))
                yield env.timeout(1)

        def slow_consumer(env):
            while True:
                item = yield buffer.get()
                yield env.timeout(3)
                log.append(('consumed', item, env.now))

        env.process(producer(env))
        env.process(slow_consumer(env))
        env.run(until=40)

        assert log == [
            ('produced', 0, 0),
            ('produced', 1, 1),
            ('produced', 2, 2),
            ('consumed', 0, 3),
            ('produced', 3, 3),
            ('produced', 4, 4),
            ('backpressure', 5, 5),
            ('consumed', 1, 6),
            ('produced', 5, 7),
            ('backpressure', 6, 8),
            ('consumed', 2, 9),
            ('produced', 6, 10),
            ('backpressure', 7, 11),
            ('consumed', 3, 12),
            ('produced', 7, 13),
            ('backpressure', 8, 14),
            ('consumed', 4, 15),
            ('produced', 8, 16),
            ('backpressure', 9, 17),
            ('consumed', 5, 18),
            ('produced', 9, 19),
            ('consumed', 6, 21),
            ('consumed', 7, 24),
            ('consumed', 8, 27),
            ('consumed', 9, 30),
        ]


# ---------------------------------------------------------------------------
# M/M/1 queue: verify steady-state metrics against queueing theory
# ---------------------------------------------------------------------------


class TestMM1Queue:
    """Single-server queue with Poisson arrivals and exponential service.

    For arrival rate λ and service rate μ, theory predicts:
        utilization ρ = λ/μ
        avg number in system L = ρ/(1-ρ)
        avg time in system W = 1/(μ-λ)

    We run long enough for steady state and check within tolerance.
    """

    def test_mm1_steady_state(self):
        import random

        random.seed(42)
        env = netsim.Environment()
        server = netsim.Resource(env, capacity=1)

        lam = 0.8  # arrival rate
        mu = 1.0  # service rate
        lam / mu
        theoretical_W = 1.0 / (mu - lam)  # avg time in system

        sojourn_times = []

        def customer(env):
            arrive = env.now
            with server.request() as req:
                yield req
                yield env.timeout(random.expovariate(mu))
            sojourn_times.append(env.now - arrive)

        def arrivals(env):
            while True:
                yield env.timeout(random.expovariate(lam))
                env.process(customer(env))

        env.process(arrivals(env))
        env.run(until=50_000)

        measured_W = sum(sojourn_times) / len(sojourn_times)

        # With ~40K samples, SE ≈ 0.025. 5% tolerance is ~100 SE — will not flake.
        assert abs(measured_W - theoretical_W) / theoretical_W < 0.05, (
            f'measured W={measured_W:.2f}, theoretical W={theoretical_W:.2f}'
        )
        assert len(sojourn_times) > 30_000


# ---------------------------------------------------------------------------
# Assembly station: needs parts from multiple suppliers (AllOf on stores)
# ---------------------------------------------------------------------------


class TestAssemblyStation:
    """Assembly requires one part from each of N suppliers before proceeding."""

    def test_assembly_waits_for_all_parts(self):
        env = netsim.Environment()
        part_a = netsim.Store(env)
        part_b = netsim.Store(env)
        part_c = netsim.Store(env)
        log = []

        def supplier(env, store, name, interval):
            i = 0
            while True:
                yield env.timeout(interval)
                yield store.put(f'{name}{i}')
                i += 1

        def assembler(env):
            while True:
                # Need one of each part
                ga = part_a.get()
                gb = part_b.get()
                gc = part_c.get()
                result = yield env.all_of([ga, gb, gc])
                a, b, c = result[ga], result[gb], result[gc]
                yield env.timeout(2)  # assembly time
                log.append(('assembled', a, b, c, env.now))

        env.process(supplier(env, part_a, 'A', 3))
        env.process(supplier(env, part_b, 'B', 4))
        env.process(supplier(env, part_c, 'C', 5))
        env.process(assembler(env))
        env.run(until=30)

        assert len(log) == 5
        # First assembly: all parts arrive by t=5 (C is slowest), assembled at t=7
        assert log[0] == ('assembled', 'A0', 'B0', 'C0', 7)


# ---------------------------------------------------------------------------
# Token bucket rate limiter
# ---------------------------------------------------------------------------


class TestTokenBucket:
    """Token bucket: tokens added at fixed rate, consumed per request.
    Requests exceeding the rate are delayed until tokens are available."""

    def test_burst_then_rate_limit(self):
        env = netsim.Environment()
        bucket = netsim.Container(env, capacity=5, init=5)  # burst of 5
        log = []

        def token_refill(env):
            """Add one token per time unit."""
            while True:
                yield env.timeout(1)
                if bucket.level < bucket.capacity:
                    yield bucket.put(1)

        def sender(env):
            """Send 8 packets as fast as tokens allow."""
            for i in range(8):
                yield bucket.get(1)  # consume a token
                log.append(('sent', i, env.now))

        env.process(token_refill(env))
        env.process(sender(env))
        env.run(until=20)

        assert log == [
            ('sent', 0, 0),
            ('sent', 1, 0),
            ('sent', 2, 0),
            ('sent', 3, 0),
            ('sent', 4, 0),
            ('sent', 5, 1),
            ('sent', 6, 2),
            ('sent', 7, 3),
        ]


# ---------------------------------------------------------------------------
# Link failover: traffic reroutes when primary link fails
# ---------------------------------------------------------------------------


class TestLinkFailover:
    """Traffic flows on primary link. On failure, reroutes to backup.
    After repair, traffic returns to primary."""

    def test_failover_and_recovery(self):
        env = netsim.Environment()
        primary = netsim.Store(env, capacity=10)
        backup = netsim.Store(env, capacity=10)
        primary_up = [True]
        log = []

        def traffic_source(env):
            for i in range(12):
                link = primary if primary_up[0] else backup
                path = 'primary' if primary_up[0] else 'backup'
                yield link.put(i)
                log.append(('sent', i, path, env.now))
                yield env.timeout(1)

        def link_consumer(env, store, name):
            while True:
                item = yield store.get()
                yield env.timeout(0.5)
                log.append(('delivered', item, name, env.now))

        def failure_event(env):
            yield env.timeout(3)
            primary_up[0] = False
            log.append(('link_down', env.now))
            yield env.timeout(5)
            primary_up[0] = True
            log.append(('link_up', env.now))

        env.process(traffic_source(env))
        env.process(link_consumer(env, primary, 'primary'))
        env.process(link_consumer(env, backup, 'backup'))
        env.process(failure_event(env))
        env.run(until=20)

        sent_events = [e for e in log if e[0] == 'sent']
        assert sent_events == [
            ('sent', 0, 'primary', 0),
            ('sent', 1, 'primary', 1),
            ('sent', 2, 'primary', 2),
            ('sent', 3, 'backup', 3),
            ('sent', 4, 'backup', 4),
            ('sent', 5, 'backup', 5),
            ('sent', 6, 'backup', 6),
            ('sent', 7, 'backup', 7),
            ('sent', 8, 'primary', 8),
            ('sent', 9, 'primary', 9),
            ('sent', 10, 'primary', 10),
            ('sent', 11, 'primary', 11),
        ]
        assert ('link_down', 3) in log
        assert ('link_up', 8) in log
