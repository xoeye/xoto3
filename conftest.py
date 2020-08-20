import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--runinteg",
        action="store_true",
        default=False,
        help="run integration tests against real infrastructure",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "integ: mark test as an integration test")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runinteg"):
        # --runinteg given in cli: do not skip integration tests
        return
    skip_integ = pytest.mark.skip(reason="need --runinteg option to run")
    for item in items:
        if "integ" in item.keywords:
            item.add_marker(skip_integ)
