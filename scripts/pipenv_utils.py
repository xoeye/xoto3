#!/usr/bin/env python
import argparse
import os
import subprocess
import glob


def find_pipenv_dir(fileorpath: str) -> str:
    fileorpath = os.path.abspath(fileorpath)
    pipenv_dir = os.path.dirname(fileorpath) if not os.path.isdir(fileorpath) else fileorpath
    while len(pipenv_dir) > 1:
        if os.path.exists(os.path.join(pipenv_dir, "Pipfile")):
            return pipenv_dir
        pipenv_dir = os.path.dirname(pipenv_dir)
    return ""


def get_pipenv_venv_site_packages(pipenv_dir: str) -> str:
    venv_output = subprocess.check_output(["pipenv", "--venv"], text=True, cwd=pipenv_dir).rstrip(
        "\n"
    )
    glob_out = glob.glob(venv_output + "/lib/python*")
    return glob_out[0] + "/site-packages/"


def get_pythonpath_for_pipfile_dir_and_venv(filepath: str) -> str:
    pipenv_dir = find_pipenv_dir(filepath)
    if pipenv_dir:
        return ":".join([pipenv_dir, get_pipenv_venv_site_packages(pipenv_dir)])
    return ""


def main():
    """Prints a PYTHONPATH including the Pipfile's own directory, if one is found"""
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    args = parser.parse_args()

    pythonpath = get_pythonpath_for_pipfile_dir_and_venv(args.path)
    if pythonpath:
        print(pythonpath)


if __name__ == "__main__":
    main()
