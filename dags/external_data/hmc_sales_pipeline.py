"""Load Hyundai Motor IR "Sales Results" yearly XLSX into BigQuery (dl_external.hmc_sales).

The IR Sales Results page publishes, per year, five Excel workbooks (sales by model, global
plant sales, export by region, US/EU retail) reached via a JSON endpoint -- not a queryable
API or login flow -- so it cannot ride the recipe -> envelope flow of
``scrape_external_data_pipeline``. Mirroring the e-Stat Final Report DAG:

1. **produce_parquet** triggers the same ``dfml-scraper`` Cloud Run Job in HMC file mode
   (``HMC_FILE_MODE=1``): it discovers the year range's workbooks, melts them to long-format
   records ``(period, year, month, dataset, group, item, value, source, retrieved_at)``, and
   writes one run-scoped parquet object to GCS (``OUTPUT_URI``).
2. **load_parquet_to_bigquery** loads that parquet into the dedicated ``dl_external.hmc_sales``
   table (delete the covered years, then append) -- idempotent for any range.

The Hyundai data is vehicle unit sales across thousands of model/region/plant series, so it
lands as a plain long-format table (the USDA PS&D pattern), not the ``dl_materials``
commodity star schema the e-Stat metrics use.

One pod per task (KubernetesExecutor): the Cloud Run Job does the heavy XLSX work off-pod and
the BigQuery load runs as one job, so each Airflow task is just an I/O orchestrator. The whole
run is idempotent -- the scraper rewrites the run's parquet and the load replaces the covered
years -- so a coarse retry safely replays it.

dag_run.conf (optional):
- start_year: first year to collect (default: ``hmc_sales_start_year`` Variable, else 2017)
- end_year:   last year to collect  (default: the run's logical-date year)
- lang:       endpoint language node (default: ko)
- dl_dataset: BigQuery landing dataset (default: dl_external)
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, Variable, get_current_context, task
from pendulum import datetime

from external_data.common.hmc_sales_bigquery import load_parquet_to_bq
from external_data.common.scrape_request import gcs_uri
from external_data.common.scraper_cloud_run import (
    SCRAPE_EXECUTION_TIMEOUT,
    execute_scraper_job,
)


# The first year the IR Sales Results endpoint serves data for (older years return empty).
DEFAULT_START_YEAR = 2017


def _required_variable(name: str) -> str:
    value = Variable.get(name, default=None)
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _scraper_config() -> dict[str, str]:
    return {
        "project_id": _required_variable("scraper_gcp_project_id"),
        "region": _required_variable("scraper_cloud_run_region"),
        "job_name": _required_variable("scraper_cloud_run_job_name"),
        "bucket": _required_variable("scraper_scrape_bucket_name"),
    }


def _bq_config() -> dict[str, str]:
    # Reuse the materials BigQuery project/region Variables (the same platform project); the
    # landing dataset defaults to dl_external (overridable via dag_run.conf).
    return {
        "project_id": _required_variable("materials_bigquery_project_id"),
        "region": _required_variable("materials_bigquery_region"),
    }


def _resolve_years(conf: dict[str, Any], logical_year: int) -> tuple[int, int]:
    start_year = int(conf.get("start_year") or Variable.get("hmc_sales_start_year", default=DEFAULT_START_YEAR))
    end_year = int(conf.get("end_year") or logical_year)
    if start_year > end_year:
        raise ValueError(f"start_year {start_year} must be <= end_year {end_year}")
    return start_year, end_year


with DAG(
    dag_id="external_data__hmc_sales",
    description="Hyundai IR Sales Results XLSX -> parquet (Cloud Run) -> BigQuery (dl_external.hmc_sales)",
    # Monthly: Hyundai updates the current year's workbooks each month. The scraper re-fetches
    # the requested range each run and the load replaces those years, so the exact day is not
    # load-bearing.
    schedule="0 10 12 * *",
    start_date=datetime(2017, 1, 1, tz="Asia/Seoul"),
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
        "hmc",
        "hyundai",
        "cloud-run",
        "bigquery",
    ],
) as dag:

    @task(execution_timeout=SCRAPE_EXECUTION_TIMEOUT)
    def produce_parquet() -> dict[str, Any]:
        """Run the scraper Cloud Run Job in HMC file mode; it writes the run's parquet to GCS."""
        context = get_current_context()
        run_id = context["run_id"]
        conf = context.get("dag_run").conf or {}
        config = _scraper_config()
        start_year, end_year = _resolve_years(conf, context["logical_date"].year)

        parquet_object = f"scrape/hmc_sales/{run_id}/sales.parquet"
        output_uri = gcs_uri(config["bucket"], parquet_object)
        # REQUEST_URI is unused in file mode (job.py dispatches before reading it) but the
        # run-request builder requires a non-empty value; pass a run-scoped placeholder.
        request_uri = gcs_uri(config["bucket"], f"scrape/hmc_sales/{run_id}/_unused_request.json")

        extra_env = {
            "HMC_FILE_MODE": "1",
            "HMC_FILE_START_YEAR": str(start_year),
            "HMC_FILE_END_YEAR": str(end_year),
        }
        if conf.get("lang"):
            extra_env["HMC_FILE_LANG"] = str(conf["lang"])

        print(f"::group::[hmc] produce parquet {start_year}-{end_year} -> {output_uri}")
        result = execute_scraper_job(
            project_id=config["project_id"],
            region=config["region"],
            job_name=config["job_name"],
            request_uri=request_uri,
            output_uri=output_uri,
            extra_env=extra_env,
        )
        print(f"[hmc] step=produce execution={result.get('execution_name')} parquet={output_uri}")
        print("::endgroup::")
        return {"parquet_uri": output_uri, "start_year": start_year, "end_year": end_year}

    @task()
    def load_parquet_to_bigquery(produced: dict[str, Any]) -> dict[str, Any]:
        """Load the run's parquet into dl_external.hmc_sales (delete covered years, append)."""
        context = get_current_context()
        conf = context.get("dag_run").conf or {}
        bq = _bq_config()
        dl_dataset = str(conf.get("dl_dataset") or "dl_external")

        rows = load_parquet_to_bq(
            parquet_uri=produced["parquet_uri"],
            gcp_project=bq["project_id"],
            location=bq["region"],
            dl_dataset=dl_dataset,
            start_year=int(produced["start_year"]),
            end_year=int(produced["end_year"]),
        )
        print(f"::group::[hmc] load -> {bq['project_id']}.{dl_dataset}.hmc_sales")
        print(f"[hmc] step=load years={produced['start_year']}-{produced['end_year']} rows={rows}")
        print("::endgroup::")
        return {"loaded_rows": rows}

    load_parquet_to_bigquery(produce_parquet())
