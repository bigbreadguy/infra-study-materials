"""pytest 수집 전에 Airflow 홈을 레포 내부로 고정 (로컬/CI에서 ~/airflow 의존 방지)."""

from __future__ import annotations

import os
from pathlib import Path

import sys

_repo_root = Path(__file__).resolve().parent.parent
_airflow_home = _repo_root / ".airflow_home_test"
os.environ.setdefault("AIRFLOW_HOME", str(_airflow_home))

_dag_folder = _repo_root / "dags"
if str(_dag_folder) not in sys.path:
    sys.path.insert(0, str(_dag_folder))
