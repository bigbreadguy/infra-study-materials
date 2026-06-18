"""Trigger the generic scraper Cloud Run Job for every KOSA scraping recipe.

One DAG run fans out across all recipes registered in SOURCES below, grouped by source
(the scraper's ``Target``: one source == one origin webpage + credential set). Each
recipe is its own build+execute task pair, so recipes are independent -- a failure or
retry of one does not block the others -- and a new source is added by appending a key
to SOURCES (it becomes its own TaskGroup automatically).

Per-recipe flow (PRD section 5): render a {schema_version, recipe, params} request ->
write it to GCS (keyed by run_id AND recipe so recipes of one run don't collide) ->
execute the Cloud Run Job with REQUEST_URI / OUTPUT_URI as per-execution env overrides
-> the synchronous operator gates the task on the job's exit code (no result-existence
sensor). Loading results GCS -> BigQuery is a separate workload.

Config comes from Airflow Variables (populate from the Terraform output
`scraper_airflow_variables`). Query params come from the per-recipe defaults below,
overridable per run via dag_run.conf. Every recipe takes a start..end month range and
returns one record per month: a scheduled run derives a single month from its logical
date (KST + lookback, start == end); pass an explicit range via conf to bulk-backfill a
span. Narrow a run to a subset with conf ``{"sources": [...]}`` and/or
``{"recipes": [...]}`` (others are skipped) -- useful to re-backfill a single recipe.
"""

from __future__ import annotations

import json
from datetime import datetime
from datetime import timedelta
from typing import Any

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, TaskGroup, Variable, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.sdk.exceptions import AirflowSkipException
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pyrefly: ignore [missing-import]
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
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
    staging_object_name,
)
from common.materials_metrics import has_materials_config, load_materials_config
from common.materials_result import envelope_records, records_to_ndjson
from common.materials_bigquery import run_materials_transform


# Registry of what to scrape, grouped by source. A "source" is the scraper's Target
# (engine/registry.py): one origin webpage + credential set, and the prefix of every
# recipe key it owns ("<source>.<flow>"). Each recipe maps to its default *query*
# params -- the filter selections specific to that recipe -- overridable per run via
# dag_run.conf. The start..end month range is shared across all recipes and resolved
# separately (see _resolve_period), so it is never listed here.
#
# Add a source -> add a key here and it becomes its own TaskGroup. Add a recipe ->
# add an entry under its source. Recipes that take only the month range carry {}.
#
# kosa.steel_scrap_import query keys: country/item are the names typed into the filter
# inputs; *_code are the per-request grid-row keys (PRD 6.1).
SOURCES: dict[str, dict[str, dict[str, Any]]] = {
    "kosa": {
        "kosa.steel_scrap_import": {
            "country": "일본",
            "country_code": "104",
            "item": "용해용철스크랩",
            "item_code": "691",
        },
        "kosa.steel_scrap_domestic": {},
        "kosa.long_products_production": {},
        "kosa.eaf_steel_production": {},
    },
}

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


def _bq_config() -> dict[str, str | None]:
    # The transform-load writes BigQuery, so it reuses the shared bigquery_*
    # Variables (same as the mongo ingestion DAGs), not the scraper_* GCS ones.
    # The dataset defaults to dl_materials (Terraform owns the dataset/tables).
    return {
        "gcp_conn_id": _required_variable("gcp_conn_id"),
        "project_id": _required_variable("bigquery_project_id"),
        "dataset_id": Variable.get(
            "materials_bigquery_dataset_id", default="dl_materials"
        ),
        "region": _required_variable("bigquery_region"),
        "impersonation_chain": _optional_variable("bigquery_impersonation_chain"),
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


def _build_params(default_query: dict[str, Any], conf: dict[str, Any]) -> dict[str, Any]:
    # Only the recipe's own query keys are overridable from conf; the shared month range
    # is resolved separately so it applies uniformly to every recipe in the run.
    query_keys = tuple(default_query)
    params = merge_params(default_query, {k: conf[k] for k in query_keys if k in conf})

    start_year, start_month, end_year, end_month = _resolve_period(conf)
    params["start_year"] = start_year
    params["start_month"] = start_month
    params["end_year"] = end_year
    params["end_month"] = end_month
    return params


def _is_selected(source: str, recipe: str, conf: dict[str, Any]) -> bool:
    """Honor optional conf allow-lists: {"sources": [...]} / {"recipes": [...]}.

    Absent (or empty) list means "all". Both narrow independently and are ANDed, so
    {"sources": ["kosa"], "recipes": ["kosa.eaf_steel_production"]} runs just that one.
    """

    sources = conf.get("sources")
    recipes = conf.get("recipes")
    if sources and source not in sources:
        return False
    if recipes and recipe not in recipes:
        return False
    return True


with DAG(
    dag_id="scrape-external-data",
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
    def build_and_write_request(
        source: str, recipe: str, default_query: dict[str, Any]
    ) -> dict[str, str]:
        conf = _dag_conf()
        # Optional conf allow-list: skip recipes/sources not selected for this run. The
        # skip propagates to this recipe's execute task (all_success), without touching
        # the other recipes' independent task pairs.
        if not _is_selected(source, recipe, conf):
            raise AirflowSkipException(f"{recipe} not selected by dag_run.conf")

        context = get_current_context()
        run_id = context["run_id"]
        config = _config()

        params = _build_params(default_query, conf)
        request = build_request_payload(recipe, params)

        request_object = request_object_name(run_id, recipe)
        result_object = result_object_name(run_id, recipe)

        gcs_hook = GCSHook(
            # pyrefly: ignore [bad-argument-type]
            gcp_conn_id=config["gcp_conn_id"],
            impersonation_chain=config["impersonation_chain"],
        )
        # Idempotent overwrite keyed by run_id + recipe: a task retry re-writes the same
        # object, and sibling recipes write to distinct objects.
        from common.gcs_object import upload_replacing_object

        upload_replacing_object(
            gcs_hook,
            # pyrefly: ignore [bad-argument-type]
            bucket_name=config["bucket"],
            object_name=request_object,
            data=json.dumps(request, ensure_ascii=False),
            mime_type="application/json",
        )

        return {
            # pyrefly: ignore [bad-argument-type]
            "request_uri": gcs_uri(config["bucket"], request_object),
            # pyrefly: ignore [bad-argument-type]
            "output_uri": gcs_uri(config["bucket"], result_object),
        }

    @task()
    def normalize_result_to_ndjson(recipe: str) -> dict[str, Any]:
        """Read the recipe's result envelope and stage it as wrapped NDJSON.

        Skips recipes with no metric mapping config (so loading is opt-in and a
        fresh clone with no configs raises no errors) and recipes whose scrape
        returned no rows. BigQuery external tables need NDJSON and the records'
        Korean keys carry spaces, so each record is wrapped under an ASCII ``row``
        column (see common/materials_result).
        """
        if not has_materials_config(recipe):
            raise AirflowSkipException(f"no materials metrics config for {recipe}")

        context = get_current_context()
        run_id = context["run_id"]
        config = _config()

        gcs_hook = GCSHook(
            # pyrefly: ignore [bad-argument-type]
            gcp_conn_id=config["gcp_conn_id"],
            impersonation_chain=config["impersonation_chain"],
        )
        result_object = result_object_name(run_id, recipe)
        raw = gcs_hook.download(
            # pyrefly: ignore [bad-argument-type]
            bucket_name=config["bucket"],
            object_name=result_object,
        )
        envelope = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        records = envelope_records(envelope)
        if not records:
            raise AirflowSkipException(f"{recipe} result has no rows to load")

        from common.gcs_object import upload_replacing_object

        staging_object = staging_object_name(run_id, recipe)
        upload_replacing_object(
            gcs_hook,
            # pyrefly: ignore [bad-argument-type]
            bucket_name=config["bucket"],
            object_name=staging_object,
            data=records_to_ndjson(records),
            mime_type="application/x-ndjson",
        )

        return {
            # pyrefly: ignore [bad-argument-type]
            "staging_uri": gcs_uri(config["bucket"], staging_object),
            "row_count": len(records),
        }

    @task()
    def load_to_bigquery(recipe: str, staging: dict[str, Any]) -> dict[str, Any]:
        """Run the recipe's whole transform-load as a single BigQuery job."""
        materials_config = load_materials_config(recipe)
        if materials_config is None:
            # Defensive: normalize already skips this case, so the load skips too.
            raise AirflowSkipException(f"no materials metrics config for {recipe}")

        bq = _bq_config()
        hook = BigQueryHook(
            # pyrefly: ignore [bad-argument-type]
            gcp_conn_id=bq["gcp_conn_id"],
            impersonation_chain=bq["impersonation_chain"],
            location=bq["region"],
        )
        client = hook.get_client(
            project_id=bq["project_id"],
            location=bq["region"],
        )
        result = run_materials_transform(
            client,
            # pyrefly: ignore [bad-argument-type]
            project_id=bq["project_id"],
            # pyrefly: ignore [bad-argument-type]
            dataset_id=bq["dataset_id"],
            # pyrefly: ignore [bad-argument-type]
            region=bq["region"],
            raw_gcs_uri=staging["staging_uri"],
            config=materials_config,
            expected_row_count=staging.get("row_count"),
        )
        return {"recipe": recipe, **result, "row_count": staging.get("row_count")}

    # Fan out by source -> recipe. Each source is a TaskGroup (one origin webpage +
    # credential set); each recipe inside it is an independent build+execute pair, so a
    # failure/retry/skip of one recipe never blocks its siblings. A new source/recipe is
    # added purely by editing SOURCES above.
    #
    # Real CloudRunExecuteJobOperator task (WI5 quick win). This was previously a
    # @task that called operator.execute(get_current_context()) by hand, which Airflow
    # 3 warns against ("execute cannot be called outside the Task Runner"): manual
    # execute() bypasses the Task Runner's render-templates / pre_execute lifecycle.
    #
    # All operator config args and `overrides` are template fields, so:
    #   - config is pulled from Airflow Variables at run time via Jinja, not via a
    #     top-level Variable.get that would hit the DB on every dag-processor parse;
    #   - the REQUEST_URI / OUTPUT_URI XComArgs resolve at run time and auto-wire the
    #     upstream dependency on the recipe's build task.
    # The operator is synchronous (deferrable defaults to False): a non-zero job exit
    # raises and fails the task -- the single gate, no result-existence sensor.
    # impersonation_chain renders to "" when the Variable is unset; the Google base
    # hook treats a falsy chain as no impersonation (auth as the connection's own SA),
    # matching the original _optional_variable -> None behavior.
    for source_name, recipes in SOURCES.items():
        with TaskGroup(group_id=source_name):
            for recipe_key, recipe_default_query in recipes.items():
                # task_id within the group; the group_id is prefixed automatically.
                flow = recipe_key.removeprefix(f"{source_name}.")
                request_uris = build_and_write_request.override(
                    task_id=f"build_request_{flow}"
                )(
                    source=source_name,
                    recipe=recipe_key,
                    default_query=recipe_default_query,
                )

                execute_scraper_job = CloudRunExecuteJobOperator(
                    task_id=f"execute_{flow}",
                    project_id="{{ var.value.scraper_gcp_project_id }}",
                    region="{{ var.value.scraper_cloud_run_region }}",
                    job_name="{{ var.value.scraper_cloud_run_job_name }}",
                    overrides={
                        "container_overrides": [
                            {
                                "env": [
                                    # pyrefly: ignore [bad-index]
                                    {"name": "REQUEST_URI", "value": request_uris["request_uri"]},
                                    # pyrefly: ignore [bad-index]
                                    {"name": "OUTPUT_URI", "value": request_uris["output_uri"]},
                                ],
                            }
                        ],
                        "task_count": 1,
                    },
                    gcp_conn_id="{{ var.value.scraper_gcp_conn_id }}",
                    impersonation_chain="{{ var.value.get('scraper_impersonation_chain', '') }}",
                )

                # Transform-load is a downstream, opt-in step: it skips recipes
                # without a gitignored materials mapping config, so a recipe runs
                # build -> execute on its own until its config lands.
                staging = normalize_result_to_ndjson.override(
                    task_id=f"normalize_{flow}"
                )(recipe=recipe_key)
                load_bq = load_to_bigquery.override(task_id=f"load_bq_{flow}")(
                    recipe=recipe_key, staging=staging
                )

                request_uris >> execute_scraper_job >> staging >> load_bq
