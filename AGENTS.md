# Development

- Setup: `bash .superset/workspace.sh setup`. Each worktree gets its own venv;
  setup does not replace the Git hooks shared by worktrees.
- Superset **Run**: `bash .superset/workspace.sh check` runs lint, types and
  all tests with coverage (`make check-ci`).
- `make qt` runs the tests without coverage or benchmarks;
  `pytest --benchmark-enable tests/test_benchmarks.py` reports timings.

Confirm suspected defects with source and a reproducer, then pin the fix with
a test that fails on the old code. Design rules live in
`tests/test_invariants.py`. Performance claims need an old-vs-new comparison
on the same quiet machine. Local checks do not replace CI.
