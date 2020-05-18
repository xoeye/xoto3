#!/usr/bin/env python
"""A pipenv runner for mypy"""

import typing as ty
import subprocess
import os
import sys

from precommit_utils import interpret_precommit_args
from pipenv_utils import find_pipenv_dir


def run_in_pipenv(cmd: str, fileorpath: str, cli_args: ty.Iterable = ()):
    """Looks for the closest Pipfile/pipenv and runs the command within that venv"""
    print(f"checking {fileorpath} with {cmd}")
    fileorpath = os.path.abspath(fileorpath)
    pipenv_dir = find_pipenv_dir(fileorpath)

    if pipenv_dir:
        fileorpath = fileorpath[len(pipenv_dir) + 1 :] if pipenv_dir != fileorpath else pipenv_dir
        full_command = ["pipenv", "run", cmd, fileorpath, *cli_args]
        return subprocess.run(
            full_command, cwd=pipenv_dir, env={**os.environ, **dict(PYTHONPATH=pipenv_dir)}
        )

    return subprocess.run([cmd, fileorpath, *cli_args])


if __name__ == "__main__":
    path_args, cli_args = interpret_precommit_args()

    cmd = cli_args[0]
    cli_args = cli_args[1:]

    for path_arg in path_args:
        cp = run_in_pipenv(cmd, path_arg, cli_args)
        if cp.returncode != 0:
            print(f"Failed {cmd} over {path_arg} with {cp.returncode}")
            sys.exit(cp.returncode)
