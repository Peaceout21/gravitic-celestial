import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

INTEGRATION_ENV_FLAG = "RUN_INTEGRATION_TESTS"


def pytest_collection_modifyitems(config, items):
    if os.environ.get(INTEGRATION_ENV_FLAG):
        return
    skip_marker = pytest.mark.skip(reason=f"Set {INTEGRATION_ENV_FLAG}=1 to run integration tests.")
    for item in items:
        test_path = Path(str(item.fspath))
        if "scripts" in test_path.parts:
            item.add_marker(skip_marker)
