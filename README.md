# NetSim

[![CI](https://github.com/networmix/NetSim/actions/workflows/python-test.yml/badge.svg?branch=main)](https://github.com/networmix/NetSim/actions/workflows/python-test.yml)

Discrete-event simulation engine. Zero dependencies. Python 3.11+.

## Install

```bash
pip install netsim
```

## Quick Start

```python
import netsim

env = netsim.Environment()

def producer(env, store):
    for i in range(5):
        yield store.put(i)
        yield env.timeout(1)

def consumer(env, store):
    while True:
        item = yield store.get()
        print(f't={env.now}: got {item}')

store = netsim.Store(env)
env.process(producer(env, store))
env.process(consumer(env, store))
env.run()
```

## Core Primitives

| Type | Purpose |
|------|---------|
| `Environment` | Simulation clock and event scheduler |
| `Event` | Something that may happen; pending → triggered → processed |
| `Timeout` | Event that auto-triggers after a delay |
| `Process` | Generator-based coroutine; is itself an Event |
| `AllOf` / `AnyOf` | Composite events (`event_a & event_b`, `event_a \| event_b`) |

## Resources

| Type | Purpose |
|------|---------|
| `Store` | FIFO queue with optional capacity |
| `FilterStore` | Store with filtered get requests |
| `PriorityStore` | Retrieves items in priority order |
| `Resource` | Mutual exclusion with capacity slots |
| `PriorityResource` | Resource with priority-ordered requests |
| `PreemptiveResource` | Higher-priority requests preempt lower-priority users |
| `Container` | Bulk quantity tracking (put/get amounts) |

## Key Patterns

### Process waits for process

A process is an event. `yield` it to wait for completion:

```python
def sub_task(env):
    yield env.timeout(5)
    return 42

def main(env):
    result = yield env.process(sub_task(env))
    assert result == 42
```

### Interrupts

```python
def worker(env):
    try:
        yield env.timeout(100)
    except netsim.Interrupt as e:
        print(f'Interrupted at t={env.now}: {e.cause}')

def interruptor(env, target):
    yield env.timeout(5)
    target.interrupt('stop')

env = netsim.Environment()
proc = env.process(worker(env))
env.process(interruptor(env, proc))
env.run()
```

### Resource contention

```python
def user(env, resource):
    with resource.request() as req:
        yield req
        yield env.timeout(1)  # hold the resource
```

## Development

```bash
make dev          # create venv, install deps, set up pre-commit
make check        # pre-commit + tests + lint
make test         # tests with coverage
make qt           # quick tests (no coverage)
make lint         # ruff + pyright
```

## License

MIT
