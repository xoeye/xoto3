#!/usr/bin/env python
import argparse
import os
import subprocess
import sys

from pipenv_utils import get_pythonpath_for_pipfile_dir_and_venv
from pylint import lint


def module_passes_lint_score_limit(path, limit, other_args=()) -> bool:
    run = lint.Run([path, *other_args], do_exit=False)
    score = run.linter.stats.global_note

    if score < limit:
        print(f"Score for {path} was {score:.03f}; less than limit {limit}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_python_module")
    parser.add_argument("--score-lower-limit", type=float, default=8.5)
    parser.add_argument("--assume-pythonpath", action="store_true")

    args, pylint_args = parser.parse_known_args()
    path = args.path_to_python_module

    if not args.assume_pythonpath:
        pythonpath = get_pythonpath_for_pipfile_dir_and_venv(path)
        if pythonpath:
            # relaunch this same process but with the new pythonpath
            new_env = {**os.environ, **dict(PYTHONPATH=pythonpath)}
            # os.execve(__file__, sys.argv + ['--assume-pythonpath'], new_env)
            replacement_args = [
                os.path.abspath(sys.argv[0]),
                args.path_to_python_module,
                "--score-lower-limit",
                str(args.score_lower_limit),
                "--assume-pythonpath",
                *pylint_args,
            ]
            print(
                f"Relaunching this process with new PYTHONPATH {pythonpath} and args {replacement_args}"
            )
            subprocess.check_call(replacement_args, env=new_env)
            return

    if not module_passes_lint_score_limit(path, args.score_lower_limit, pylint_args):
        raise ValueError(f"Module {path} failed linting!")


if __name__ == "__main__":
    main()
