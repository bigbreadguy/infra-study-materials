"""Trigger the generic scraper Cloud Run Job for the kosa.steel_scrap_import recipe.

Flow (PRD section 5): render a {schema_version, recipe, params} request -> write it
to GCS -> execute the Cloud Run Job with REQUEST_URI / OUTPUT_URI as per-execution
env overrides -> the synchronous operator gates the task on the job's exit code (no
result-existence sensor). Loading results GCS -> BigQuery is a separate workload.

Config comes from Airflow Variables (populate from the Terraform output
`scraper_airflow_variables`). Query params come from defaults below, overridable per
run via dag_run.conf. The recipe takes a start..end month range and returns one record
per month: a scheduled run derives a single month from its logical date (KST +
lookback, start == end); pass an explicit range via conf to bulk-backfill a span.
"""

from __future__ import annotations

import json
from datetime import datetime
from datetime import timedelta
from typing import Any

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, Variable, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

from common.scrape_request import (
    build_request_payload,
    gcs_uri,
    merge_params,
    request_object_name,
    resolve_year_month,
    result_object_name,
)


RECIPE = "kosa.steel_scrap_import"

# Default query for kosa.steel_scrap_import. country/item are the names typed into the
# filters; *_code are the per-request grid-row keys (PRD 6.1). Override any of these via
# dag_run.conf. The date span is a start..end month range (see _resolve_period): a
# scheduled run derives a single month (start == end); pass {start_year, start_month,
# end_year, end_month} to bulk-backfill a span, or {year, month} for one month.
DEFAULT_QUERY = {
    "country": "일본",
    "country_code": "104",
    "item": "용해용철스크랩",
    "item_code": "691",
}
QUERY_KEYS = tuple(DEFAULT_QUERY)
DEFAULT_LOOKBACK_MONTHS = 0


def _required_variable(name: str) -> str:
    value = Variable.get(name, default=None)
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _optional_variable(name: str) -> str | None:
    value = Variable.get(name, default=None)
    return value or None


def _config() -> dict[str, str | None]:
    return {
        "gcp_conn_id": _required_variable("scraper_gcp_conn_id"),
        "project_id": _required_variable("scraper_gcp_project_id"),
        "region": _required_variable("scraper_cloud_run_region"),
        "job_name": _required_variable("scraper_cloud_run_job_name"),
        "bucket": _required_variable("scraper_scrape_bucket_name"),
        "impersonation_chain": _optional_variable("scraper_impersonation_chain"),
    }


def _dag_conf() -> dict[str, Any]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    return dict(getattr(dag_run, "conf", None) or {})


def _run_point_kst():
    # Airflow 3: a manually-triggered run can have logical_date=None and no data
    # interval (scheduled @monthly runs do carry one). Fall back through the run's
    # timestamp, then to now, instead of failing.
    import pendulum

    context = get_current_context()
    dag_run = context.get("dag_run")
    run_point = (
        context.get("logical_date")
        or context.get("data_interval_start")
        or getattr(dag_run, "run_after", None)
        or getattr(dag_run, "logical_date", None)
        or pendulum.now("UTC")
    )
    # context datetimes are pendulum-aware; normalize to KST before deriving month.
    return run_point.in_timezone("Asia/Seoul")


def _resolve_period(conf: dict[str, Any]) -> tuple[int, int, int, int]:
    """Return (start_year, start_month, end_year, end_month) for the request.

    The kosa recipes take a start..end month range and return one record per month
    (bulk backfill). Precedence:

    1. **Explicit range** ``{"start_year","start_month",[ "end_year","end_month" ]}``
       in conf -> used as-is (end defaults to start). This is the bulk-backfill path;
       e.g. one run with 2001-01..now replaces ~294 single-month runs.
    2. **Single month** ``{"year","month"}`` in conf -> ``start == end``.
    3. **Derived** from the run's date (KST) minus ``lookback_months`` -> ``start ==
       end``; the per-month behavior of the @monthly schedule.
    """

    if "start_year" in conf and "start_month" in conf:
        start_year = int(conf["start_year"])
        start_month = int(conf["start_month"])
        end_year = int(conf.get("end_year", start_year))
        end_month = int(conf.get("end_month", start_month))
        return start_year, start_month, end_year, end_month

    if "year" in conf and "month" in conf:
        year, month = int(conf["year"]), int(conf["month"])
    else:
        lookback = int(conf.get("lookback_months", DEFAULT_LOOKBACK_MONTHS))
        run_point = _run_point_kst()
        year, month = resolve_year_month(run_point.year, run_point.month, lookback)
    return year, month, year, month


def _build_params() -> dict[str, Any]:
    conf = _dag_conf()
    params = merge_params(DEFAULT_QUERY, {k: conf[k] for k in QUERY_KEYS if k in conf})

    start_year, start_month, end_year, end_month = _resolve_period(conf)
    params["start_year"] = start_year
    params["start_month"] = start_month
    params["end_year"] = end_year
    params["end_month"] = end_month
    return params


with DAG(
    dag_id="scrape-kosa-steel-scrap",
    # The KOSA steel-scrap series starts 2001-01 and is month-grained. start_date marks
    # the earliest schedulable month; catchup=False keeps the scheduler from auto-firing
    # the ~294 historical months on unpause -- the historical backfill is a deliberate,
    # separate run (set catchup=True or `airflow dags backfill` when ready).
    start_date=datetime(2001, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["scraper", "kosa", "cloud-run", "external-data"],
) as dag:

    @task()
    def build_and_write_request() -> dict[str, str]:
        context = get_current_context()
        run_id = context["run_id"]
        config = _config()

        params = _build_params()
        request = build_request_payload(RECIPE, params)

        request_object = request_object_name(run_id)
        result_object = result_object_name(run_id)

        gcs_hook = GCSHook(
            gcp_conn_id=config["gcp_conn_id"],
            impersonation_chain=config["impersonation_chain"],
        )
        # Idempotent overwrite keyed by run_id: a task retry re-writes the same object.
        from common.gcs_object import upload_replacing_object

        upload_replacing_object(
            gcs_hook,
            bucket_name=config["bucket"],
            object_name=request_object,
            data=json.dumps(request, ensure_ascii=False),
            mime_type="application/json",
        )

        return {
            "request_uri": gcs_uri(config["bucket"], request_object),
            "output_uri": gcs_uri(config["bucket"], result_object),
        }

    request_uris = build_and_write_request()

    # Real CloudRunExecuteJobOperator task (WI5 quick win). This was previously a
    # @task that called operator.execute(get_current_context()) by hand, which Airflow
    # 3 warns against ("execute cannot be called outside the Task Runner"): manual
    # execute() bypasses the Task Runner's render-templates / pre_execute lifecycle.
    #
    # All operator config args and `overrides` are template fields, so:
    #   - config is pulled from Airflow Variables at run time via Jinja, not via a
    #     top-level Variable.get that would hit the DB on every dag-processor parse;
    #   - the REQUEST_URI / OUTPUT_URI XComArgs resolve at run time and auto-wire the
    #     upstream dependency on build_and_write_request.
    # The operator is synchronous (deferrable defaults to False): a non-zero job exit
    # raises and fails the task -- the single gate, no result-existence sensor.
    # impersonation_chain renders to "" when the Variable is unset; the Google base
    # hook treats a falsy chain as no impersonation (auth as the connection's own SA),
    # matching the original _optional_variable -> None behavior.
    execute_scraper_job = CloudRunExecuteJobOperator(
        task_id="execute_scraper_job",
        project_id="{{ var.value.scraper_gcp_project_id }}",
        region="{{ var.value.scraper_cloud_run_region }}",
        job_name="{{ var.value.scraper_cloud_run_job_name }}",
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {"name": "REQUEST_URI", "value": request_uris["request_uri"]},
                        {"name": "OUTPUT_URI", "value": request_uris["output_uri"]},
                    ],
                }
            ],
            "task_count": 1,
        },
        gcp_conn_id="{{ var.value.scraper_gcp_conn_id }}",
        impersonation_chain="{{ var.value.get('scraper_impersonation_chain', '') }}",
    )

    request_uris >> execute_scraper_job
