"""Transform glue: the job config must register the temp external table under the
same name the cosmetics SQL reads from (a mismatch surfaces at runtime as BigQuery
400 "Table ... must be qualified with a dataset")."""

from __future__ import annotations

import pytest

from external_data.common import cosmetics_bigquery_sql as sql
from external_data.common.cosmetics_bigquery import run_cosmetics_transform


class _FakeJob:
    job_id = "job-cosmetics-001"
    location = "asia-northeast3"
    num_dml_affected_rows = None

    def result(self):
        return iter(())


class _FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, query_sql, *, location, job_config, job_id_prefix=None):
        self.queries.append(
            {"sql": query_sql, "location": location,
             "job_config": job_config, "job_id_prefix": job_id_prefix}
        )
        return _FakeJob()


def test_transform_registers_temp_table_under_the_sql_name():
    pytest.importorskip("google.cloud.bigquery")
    client = _FakeClient()

    run_cosmetics_transform(
        client,
        project_id="example-proj",
        landing_dataset_id="dl_cosmetics",
        star_dataset_id="dm_cosmetics",
        region="asia-northeast3",
        raw_gcs_uri="gs://bucket/scrape/staging/cosmetics/run-1/*.ndjson",
    )

    (query,) = client.queries
    definitions = query["job_config"].table_definitions
    assert list(definitions) == [sql.RAW_RECORDS_TABLE]
    # and the SQL actually reads from that table
    assert sql.RAW_RECORDS_TABLE in query["sql"]
