"""Hyundai Sales Results BigQuery load-helper tests (no BigQuery, no Airflow)."""

from __future__ import annotations

from pathlib import Path

import pytest

from external_data.common.hmc_sales_bigquery import (
    BQ_SCHEMA,
    BQ_TABLE_ID,
    delete_years_sql,
    load_parquet_to_bq,
    table_fqn,
)


def test_schema_matches_scraper_parquet_columns():
    # The schema must mirror the scraper's parquet columns (pipelines.hmc_sales._PARQUET_COLUMNS).
    names = [f.name for f in BQ_SCHEMA]
    assert names == [
        "period", "year", "month", "dataset", "group",
        "item", "value", "source", "retrieved_at",
    ]
    by_name = {f.name: f.field_type for f in BQ_SCHEMA}
    assert by_name["value"] == "FLOAT64"
    assert by_name["year"] == "INT64"
    assert by_name["month"] == "INT64"
    # The scraper writes retrieved_at as a parquet timestamp -> native BigQuery TIMESTAMP.
    assert by_name["retrieved_at"] == "TIMESTAMP"


def test_table_fqn_and_delete_sql():
    assert table_fqn("proj", "dl_external") == f"proj.dl_external.{BQ_TABLE_ID}"
    sql = delete_years_sql("proj", "dl_external")
    assert "DELETE FROM `proj.dl_external.hmc_sales`" in sql
    assert "year BETWEEN @start_year AND @end_year" in sql


def test_load_rejects_non_gs_uri():
    with pytest.raises(ValueError):
        load_parquet_to_bq(
            parquet_uri="/local/sales.parquet",
            gcp_project="p", location="asia-northeast3",
            dl_dataset="dl_external", start_year=2024, end_year=2024,
            client=object(),  # never reached
        )


def test_load_rejects_inverted_year_range():
    with pytest.raises(ValueError):
        load_parquet_to_bq(
            parquet_uri="gs://b/x.parquet",
            gcp_project="p", location="asia-northeast3",
            dl_dataset="dl_external", start_year=2025, end_year=2024,
            client=object(),
        )


# --- DAG source guard (mirrors the dpanda DAG test; no Airflow import needed) -----

_DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "external_data" / "hmc_sales_pipeline.py"


def test_dag_uses_adc_no_hooks_or_impersonation():
    source = _DAG_FILE.read_text()
    # The GKE cluster defines no Airflow GCP connection; GCP is reached via google.cloud
    # clients (ADC / workload identity), so the DAG carries no hooks, conn ids, or impersonation.
    for forbidden in ("GCSHook", "BigQueryHook", "gcp_conn_id", "impersonation_chain"):
        assert forbidden not in source
    # It drives the shared scraper job in HMC file mode and lands in the dedicated table.
    assert "HMC_FILE_MODE" in source
    assert "external_data__hmc_sales" in source
