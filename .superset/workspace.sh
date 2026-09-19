#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mode=${1:-help}
case "$mode" in
    setup|check) ;;
    *) echo 'Usage: bash .superset/workspace.sh {setup|check}'; exit 2 ;;
esac
if [[ "$mode" == setup && ! -x venv/bin/python ]]; then
    make venv
fi
[[ -x venv/bin/python ]] || { echo 'Run bash .superset/workspace.sh setup first.' >&2; exit 1; }
export VIRTUAL_ENV="$PWD/venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"
unset PYTHONPATH PYTHONHOME
if [[ "$mode" == setup ]]; then
    python -m pip install -e '.[dev]'
    python -m pip check
    python -c 'import netsim; print("Workspace environment ready")'
else
    make check-ci
fi
