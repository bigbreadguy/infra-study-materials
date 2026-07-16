"""Load the e-Stat Petroleum Statistics Final Report (file-only survey 00551030) into BigQuery.

The Final Report survey has no e-Stat database tables -- only monthly Excel files -- so it
cannot ride the recipe -> envelope flow of ``scrape_external_data_pipeline``. Instead:

1. **produce_parquet** triggers the same ``dfml-scraper`` Cloud Run Job in file mode
   (``ESTAT_FILE_MODE=1``): it catalogs the monthly product-table XLS, parses every metric,
   and writes one run-scoped parquet object to GCS (``OUTPUT_URI``). Each record is
   ``{"row": <json>}`` with the plain-English long-format fields
   (``period, metric, value, unit, source, stat_inf_id, retrieved_at``).
2. **load_parquet_to_bigquery** loads that parquet into the ``dl_materials`` star schema via
   the shared materials transform (``source_format="PARQUET"``) under the **authoritative**
   ``estat_kakuho`` datasource, covering all five metrics incl. naphtha production (which the
   DB survey lacks). ``dim_metrics`` keys by (datasource, name), so these coexist with the
   timelier DB ``estat`` metrics; the Final Report figures are the finalized ones we trust/use.

One pod per task (KubernetesExecutor): the Cloud Run Job does the heavy XLS work off-pod and
the BigQuery transform runs as one job, so each Airflow task is just an I/O orchestrator. The
whole run is idempotent -- the scraper rewrites the run's parquet object and the fact_values
MERGE dedups -- so a coarse retry safely replays it.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, Variable, get_current_context, task
from pendulum import datetime

from external_data.common.materials_bigquery import run_combined_materials_transform
from external_data.common.materials_metrics import load_materials_config
from external_data.common.scrape_request import gcs_uri
from external_data.common.scraper_cloud_run import (
    SCRAPE_EXECUTION_TIMEOUT,
    execute_scraper_job,
)


# The five Final Report file metrics -> their estat_kakuho materials config stems. The metric names
# are the parquet ``metric`` values the configs match on (see configs/.../estat_kakuho/).
ESTAT_KAKUHO_CONFIGS = [
    "estat_kakuho.crude_oil_imports",
    "estat_kakuho.gasoline_consumption",
    "estat_kakuho.kerosene_imports",
    "estat_kakuho.naphtha_production",
    "estat_kakuho.lpg_consumption",
]

# Airflow Variable holding the e-Stat appId (shared with the DB estat source's
# SOURCE_SECRET_ENV in scrape_external_data_pipeline).
ESTAT_APP_ID_VARIABLE = "scraper_estat_app_id"

_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")
_JOB_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")


# --- Config helpers (mirror scrape_external_data_pipeline; GCP access via ADC) ------


def _required_variable(name: str) -> str:
    value = Variable.get(name, default=None)
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _config() -> dict[str, str]:
    return {
        "project_id": _required_variable("scraper_gcp_project_id"),
        "region": _required_variable("scraper_cloud_run_region"),
        "job_name": _required_variable("scraper_cloud_run_job_name"),
        "bucket": _required_variable("scraper_scrape_bucket_name"),
    }


def _bq_config() -> dict[str, str]:
    return {
        "project_id": _required_variable("materials_bigquery_project_id"),
        "dataset_id": Variable.get("materials_bigquery_dataset_id", default="dl_materials"),
        "region": _required_variable("materials_bigquery_region"),
    }


def _label_value(value: Any) -> str:
    return _LABEL_DISALLOWED.sub("_", str(value).lower())[:63]


def _parquet_object_name(run_id: str) -> str:
    # Run-scoped so concurrent/historic runs never clobber each other; rewritten in place
    # on retry (idempotent).
    return f"scrape/estat_file/{run_id}/petroleum.parquet"


with DAG(
    dag_id="external_data__estat_file",
    description="e-Stat Petroleum Statistics Final Report XLS -> parquet (Cloud Run) -> BigQuery (estat_kakuho)",
    # Monthly: the Final Report publishes finalized monthly figures with a lag. The scraper re-fetches
    # the full history each run and the MERGE dedups, so the exact day is not load-bearing.
    schedule="0 10 10 * *",
    start_date=datetime(2015, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=[
        "external_data",
        "scraper",
        "estat",
        "estat_kakuho",
        "cloud-run",
        "bigquery",
    ],
) as dag:

    @task(execution_timeout=SCRAPE_EXECUTION_TIMEOUT)
    def produce_parquet() -> dict[str, Any]:
        """Run the scraper Cloud Run Job in file mode; it writes the run's parquet to GCS."""
        context = get_current_context()
        run_id = context["run_id"]
        config = _config()
        app_id = _required_variable(ESTAT_APP_ID_VARIABLE)

        parquet_object = _parquet_object_name(run_id)
        output_uri = gcs_uri(config["bucket"], parquet_object)
        # REQUEST_URI is unused in file mode (job.py dispatches before reading it) but the
        # run-request builder requires a non-empty value; pass a run-scoped placeholder.
        request_uri = gcs_uri(config["bucket"], f"scrape/estat_file/{run_id}/_unused_request.json")

        params = context.get("params") or {}
        extra_env = {"ESTAT_FILE_MODE": "1", "ESTAT_APP_ID": app_id}
        if params.get("start"):
            extra_env["ESTAT_FILE_START"] = str(params["start"])
        if params.get("end"):
            extra_env["ESTAT_FILE_END"] = str(params["end"])

        print(f"::group::[estat-file] produce parquet -> {output_uri}")
        result = execute_scraper_job(
            project_id=config["project_id"],
            region=config["region"],
            job_name=config["job_name"],
            request_uri=request_uri,
            output_uri=output_uri,
            extra_env=extra_env,
        )
        print(f"[estat-file] step=produce execution={result.get('execution_name')} parquet={output_uri}")
        print("::endgroup::")
        return {"parquet_uri": output_uri}

    @task()
    def load_parquet_to_bigquery(produced: dict[str, Any]) -> dict[str, Any]:
        """Load the run's parquet into fact_values via the shared materials transform."""
        from google.cloud import bigquery

        context = get_current_context()
        bq = _bq_config()
        raw_gcs_uri = produced["parquet_uri"]
        configs = [load_materials_config(stem) for stem in ESTAT_KAKUHO_CONFIGS]

        labels = {
            "dag_id": _label_value(getattr(context.get("dag_run"), "dag_id", "") or ""),
            "run_id": _label_value(context.get("run_id", "")),
            "source": "estat_kakuho",
            "step": "load",
        }
        job_id_prefix = _JOB_ID_DISALLOWED.sub(
            "_", f"estat_kakuho_{context.get('run_id', '')}_load_"
        )[:512]

        client = bigquery.Client(project=bq["project_id"], location=bq["region"])
        result = run_combined_materials_transform(
            client,
            project_id=bq["project_id"],
            dataset_id=bq["dataset_id"],
            region=bq["region"],
            raw_gcs_uri=raw_gcs_uri,
            configs=configs,
            # The parquet row count is not known to the DAG (the scraper wrote it off-pod),
            # so no expected_row_count assert; the MERGE is idempotent regardless.
            expected_row_count=None,
            source_format="PARQUET",
            labels=labels,
            job_id_prefix=job_id_prefix,
        )
        merged = result.get("dml_affected_rows")
        print(f"::group::[estat-file] load ({len(configs)} metrics)")
        print(
            f"[estat-file] step=load metrics={len(configs)} merged={merged} "
            f"job_id={result.get('job_id')}"
        )
        print("::endgroup::")
        return {"merged_rows": merged, "job_id": result.get("job_id")}

    load_parquet_to_bigquery(produce_parquet())
