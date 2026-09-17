from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

INFRA_DIR = Path(__file__).resolve().parents[1]
DLT_INFRA_DIR = INFRA_DIR / "dlt"
for path in (INFRA_DIR, DLT_INFRA_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def load_infra_module(module_name: str, relative_path: str):
    path = INFRA_DIR / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def dbt_alerts():
    return load_infra_module("dbt_alerts", "dbt/alerts.py")


@pytest.fixture(scope="session")
def dlt_alerts():
    return load_infra_module("dlt_alerts", "dlt/alerts.py")


@pytest.fixture(scope="session")
def dlt_process():
    return load_infra_module("dlt_process", "dlt/process.py")
