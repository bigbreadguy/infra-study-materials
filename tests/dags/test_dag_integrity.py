"""PR에서 DAG 폴더 import 무결성 검사."""

import sys
from pathlib import Path

from airflow.models.dagbag import DagBag


def test_dag_bag_has_no_import_errors():
    repo_root = Path(__file__).resolve().parent.parent.parent
    dag_folder = repo_root / "dags"
    if str(dag_folder) not in sys.path:
        sys.path.insert(0, str(dag_folder))
    bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
    assert not bag.import_errors, bag.import_errors


# duty_free 트리는 이 테스트베드로 마이그레이션하지 않았으므로
# external_data DAG 발견만 검증한다.
def test_external_data_dags_are_discovered():
    repo_root = Path(__file__).resolve().parent.parent.parent
    dag_folder = repo_root / "dags"
    if str(dag_folder) not in sys.path:
        sys.path.insert(0, str(dag_folder))
    bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
    assert "external_data__usda_psd_oilseeds" in bag.dags
    assert "external_data__dpanda_bloomberg__market_macro" in bag.dags
    assert "external_data__dpanda_bloomberg__nickel" in bag.dags
    assert "external_data__dpanda_bloomberg__backfill" in bag.dags
    assert "external_data__dpanda_grain_metadata" in bag.dags
    assert "external_data__scrape_external_data" in bag.dags
    assert "external_data__cochilco_grades" in bag.dags
