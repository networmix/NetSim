"""Optional NetGraph-backed command line entry point."""

from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='netsim')
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser(
        'run', help='run a NetGraph scenario with NetSimStudy steps'
    )
    run.add_argument('scenario', type=Path)
    run.add_argument('--results', type=Path, default=Path('results.json'))
    args = parser.parse_args(argv)
    try:
        scenario_module = importlib.import_module('ngraph.scenario')
    except ModuleNotFoundError as exc:
        if exc.name != 'ngraph':
            raise
        parser.error('netsim run requires the optional ngraph package')
    importlib.import_module('netsim.adapters.ngraph')  # Register before YAML parsing.
    scenario = scenario_module.Scenario.from_yaml(args.scenario.read_text())
    scenario.run()
    args.results.write_text(
        json.dumps(scenario.results.to_dict(), indent=2, default=str) + '\n'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
