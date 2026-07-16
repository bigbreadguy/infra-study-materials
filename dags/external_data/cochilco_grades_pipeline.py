"""Load Chilean copper-mining grades (Cochilco anuario) into BigQuery (dl_materials).

Cochilco publishes its "Anuario de Estadísticas del Cobre y Otros Minerales" as one Excel
workbook per year (a ~150-sheet statistical yearbook), not a queryable API -- so it cannot
ride the recipe -> envelope flow of ``scrape_external_data_pipeline``. Mirroring the e-Stat
Final Report DAG:

1. **produce_parquet** triggers the same ``dfml-scraper`` Cloud Run Job in Cochilco file mode
   (``COCHILCO_FILE_MODE=1``, ``COCHILCO_FILE_BACKFILL=1``): it finds the copper-mining-grades
   table by title (its ``Tabla`` *number* drifts 47/48/50 across reports, the title does not),
   walks back report by report using each report's actual ~10-year grades window to cover the
   whole history from the fewest files, maps the four process series to canonical metrics
   (``grade_concentrator`` / ``grade_heap_leach`` / ``grade_dump_rom`` / ``grade_chile_average``;
   the pre-2013 merged "Lixiviación" row is heap leaching only), dedups to the newest report per
   ``(metric, year)``, and writes one run-scoped parquet to GCS in the materials row contract
   (``{"row": <json>}`` with ``period`` = ``YYYY-01-01``, ``metric``, ``value``, ``unit``).
2. **load_parquet_to_bigquery** loads that parquet into the ``dl_materials`` star schema via the
   shared materials transform (``source_format="PARQUET"``) under the ``cochilco`` datasource,
   one annual (``time_grain="Y"``) metric per grade series.

One pod per task (KubernetesExecutor): the Cloud Run Job does the heavy XLSX work off-pod and
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


# The four copper-mining-grade series -> their cochilco materials config stems. The scraper's
# parquet ``metric`` values (grade_*) are what the configs match on (see configs/.../cochilco/).
COCHILCO_GRADES_CONFIGS = [
    "cochilco.grade_concentrator",
    "cochilco.grade_heap_leach",
    "cochilco.grade_dump_rom",
    "cochilco.grade_chile_average",
]

_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")
_JOB_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")


# --- Config helpers (mirror estat_file_pipeline; GCP access via ADC) ----------------


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
    return f"scrape/cochilco_grades/{run_id}/grades.parquet"


with DAG(
    dag_id="external_data__cochilco_grades",
    description="Cochilco copper mining grades XLSX -> parquet (Cloud Run) -> BigQuery (dl_materials, cochilco)",
    # Quarterly: the anuario publishes a new edition once a year (mid-year). The scraper re-pulls
    # the full grades history each run and the MERGE dedups, so the exact day is not load-bearing;
    # quarterly catches the new edition within a quarter at negligible cost.
    schedule="0 10 5 1,4,7,10 *",
    start_date=datetime(2009, 1, 1, tz="Asia/Seoul"),
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
        "cochilco",
        "copper",
        "cloud-run",
        "bigquery",
    ],
) as dag:

    @task(execution_timeout=SCRAPE_EXECUTION_TIMEOUT)
    def produce_parquet() -> dict[str, Any]:
        """Run the scraper Cloud Run Job in Cochilco file mode; it writes the run's parquet to GCS."""
        context = get_current_context()
        run_id = context["run_id"]
        config = _config()

        parquet_object = _parquet_object_name(run_id)
        output_uri = gcs_uri(config["bucket"], parquet_object)
        # REQUEST_URI is unused in file mode (job.py dispatches before reading it) but the
        # run-request builder requires a non-empty value; pass a run-scoped placeholder.
        request_uri = gcs_uri(config["bucket"], f"scrape/cochilco_grades/{run_id}/_unused_request.json")

        # The whole grades history from the fewest files: file mode + windowed backfill.
        extra_env = {"COCHILCO_FILE_MODE": "1", "COCHILCO_FILE_BACKFILL": "1"}

        print(f"::group::[cochilco] produce parquet -> {output_uri}")
        result = execute_scraper_job(
            project_id=config["project_id"],
            region=config["region"],
            job_name=config["job_name"],
            request_uri=request_uri,
            output_uri=output_uri,
            extra_env=extra_env,
        )
        print(f"[cochilco] step=produce execution={result.get('execution_name')} parquet={output_uri}")
        print("::endgroup::")
        return {"parquet_uri": output_uri}

    @task()
    def load_parquet_to_bigquery(produced: dict[str, Any]) -> dict[str, Any]:
        """Load the run's parquet into fact_values via the shared materials transform."""
        from google.cloud import bigquery

        context = get_current_context()
        bq = _bq_config()
        raw_gcs_uri = produced["parquet_uri"]
        configs = [load_materials_config(stem) for stem in COCHILCO_GRADES_CONFIGS]

        labels = {
            "dag_id": _label_value(getattr(context.get("dag_run"), "dag_id", "") or ""),
            "run_id": _label_value(context.get("run_id", "")),
            "source": "cochilco",
            "step": "load",
        }
        job_id_prefix = _JOB_ID_DISALLOWED.sub(
            "_", f"cochilco_{context.get('run_id', '')}_load_"
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
        print(f"::group::[cochilco] load ({len(configs)} grade metrics)")
        print(
            f"[cochilco] step=load metrics={len(configs)} merged={merged} "
            f"job_id={result.get('job_id')}"
        )
        print("::endgroup::")
        return {"merged_rows": merged, "job_id": result.get("job_id")}

    load_parquet_to_bigquery(produce_parquet())
