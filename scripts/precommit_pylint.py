#!/usr/bin/env python
import subprocess
import os

from precommit_utils import interpret_precommit_args
from pipenv_utils import get_pythonpath_for_pipfile_dir_and_venv


if __name__ == "__main__":
    path_args, cli_args = interpret_precommit_args()

    for path in path_args:
        pythonpath = get_pythonpath_for_pipfile_dir_and_venv(path)
        print(f"Linting {path} with PYTHONPATH <{pythonpath}>")
        subprocess.check_call(
            ["scripts/pylint_pipenv_score_limit.py", path, *cli_args, "--assume-pythonpath"],
            env={**os.environ, **dict(PYTHONPATH=pythonpath)},
        )
