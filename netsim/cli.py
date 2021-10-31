#! /root/env/env/bin/python3
import argparse
from typing import Dict, Union

from netsim.utils import yaml_to_dict
from netsim.workflow import Workflow


def parse_args() -> Dict[str, Union[str, int, float, bool]]:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("wf_path", help="Path to workflow")
    return vars(parser.parse_args())


def main() -> None:
    args = parse_args()
    with open(args["wf_path"], encoding="utf-8") as f:
        blueprint_dict = yaml_to_dict(f.read())
    wf = Workflow.from_dict(blueprint_dict)
    wf.run()


if __name__ == "__main__":
    main()
