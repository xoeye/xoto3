# type: ignore
"""Common code use by unit tests in different vision-nx projects"""

import importlib
from os.path import basename, isfile, join, isdir, dirname
import glob


def import_all(file: str, module: str):
    """Test all files in the module are importable"""
    _import_dir(join(dirname(dirname(file)), module), module)


def _import_dir(current_dir: str, module):
    """Go though a directory and import all the python modules """
    directory = glob.glob(join(current_dir, "*"))
    for element in directory:
        if isfile(element) and basename(element)[-3:] == ".py":
            module_full = f"{module}.{basename(element[:-3])}"
            importlib.import_module(module_full)
        elif isdir(element):
            pkg = f"{module}.{basename(element)}"
            _import_dir(element, pkg)


ROOT_MODULE = "xoto3"


def test_imports():
    """Test all files in the module are importable"""
    import_all(__file__, ROOT_MODULE)
