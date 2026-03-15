"""Tests for process interruption."""

import pytest

import netsim


class TestInterruptWaitingProcess:
    def test_interrupt_a_waiting_process(self):
        env = netsim.Environment()
        log = []

        def victim(env):
            try:
                yield env.timeout(10)
            except netsim.Interrupt as e:
                log.append(('interrupted', env.now, e.cause))

        def attacker(env, target):
            yield env.timeout(1)
            target.interrupt('go away')

        v = env.process(victim(env))
        env.process(attacker(env, v))
        env.run()
        assert log == [('interrupted', 1, 'go away')]


class TestInterruptCause:
    def test_cause_accessible(self):
        env = netsim.Environment()
        causes = []

        def victim(env):
            try:
                yield env.timeout(100)
            except netsim.Interrupt as e:
                causes.append(e.cause)

        def attacker(env, target):
            yield env.timeout(1)
            target.interrupt({'reason': 'test'})

        v = env.process(victim(env))
        env.process(attacker(env, v))
        env.run()
        assert causes == [{'reason': 'test'}]

    def test_cause_default_is_none(self):
        env = netsim.Environment()
        causes = []

        def victim(env):
            try:
                yield env.timeout(100)
            except netsim.Interrupt as e:
                causes.append(e.cause)

        def attacker(env, target):
            yield env.timeout(1)
            target.interrupt()

        v = env.process(victim(env))
        env.process(attacker(env, v))
        env.run()
        assert causes == [None]


class TestInterruptDeadProcess:
    def test_interrupt_dead_process_raises(self):
        env = netsim.Environment()

        def short_lived(env):
            yield env.timeout(1)

        p = env.process(short_lived(env))
        env.run()
        with pytest.raises(RuntimeError, match='terminated'):
            p.interrupt('too late')


class TestSelfInterrupt:
    def test_self_interrupt_raises(self):
        env = netsim.Environment()

        def proc(env):
            myself = env.active_process
            myself.interrupt('self')
            yield env.timeout(1)

        env.process(proc(env))
        with pytest.raises(RuntimeError, match='not allowed to interrupt itself'):
            env.run()


class TestConcurrentInterrupts:
    def test_first_handled_second_ignored_if_process_dies(self):
        """If two _Interruption events are scheduled before either is
        processed, the second is silently ignored because the victim is
        already dead when the second _Interruption callback fires."""
        env = netsim.Environment()
        log = []

        def victim(env):
            try:
                yield env.timeout(100)
            except netsim.Interrupt as e:
                log.append(e.cause)
                return  # Process dies

        def attacker(env, target):
            """Schedule both interrupts in a single process so they are
            both queued before either is processed."""
            yield env.timeout(1)
            target.interrupt('first')
            target.interrupt('second')

        v = env.process(victim(env))
        env.process(attacker(env, v))
        env.run()
        # Only "first" is delivered; "second" is silently dropped by
        # _Interruption._interrupt because the victim is already dead.
        assert log == ['first']


class TestDefusedInterrupt:
    def test_defused_interrupt(self):
        """If an interrupt event is defused, it should not propagate."""
        env = netsim.Environment()
        log = []

        def victim(env):
            try:
                yield env.timeout(100)
            except netsim.Interrupt as e:
                log.append(e.cause)
                yield env.timeout(1)
                log.append('continued')

        def attacker(env, target):
            yield env.timeout(1)
            target.interrupt('hey')

        v = env.process(victim(env))
        env.process(attacker(env, v))
        env.run()
        assert 'hey' in log
        assert 'continued' in log
