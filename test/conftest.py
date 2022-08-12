import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--realjobs", action="store_true", default=False,
        help="run tests with real jobs (must be on a platform that has HTCondor installed)"
        )

def pytest_configure(config):
    config.addinivalue_line("markers", "realjobs: mark tests that run actual jobs via HTCondor")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--realjobs"):
        # --realjobs given in cli: do not skip realjobs tests
        return
    skip_realjobs = pytest.mark.skip(reason="need --realjobs option to run")
    for item in items:
        if "realjobs" in item.keywords:
            item.add_marker(skip_realjobs)