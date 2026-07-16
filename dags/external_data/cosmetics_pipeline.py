"""``external_data__cosmetics`` — one dual-mode DAG: daily scrape + manual backfill + load.

**Scheduled runs** scrape the five live cosmetics recipes through the Cloud Run scraper job
(one mapped task per recipe: independent per-site retry of a long/flaky operation, plan §2.2).
**Manual runs with backfill params** normalize the historical prototype snapshots under
``cnp-scraping-raw/`` into the same result-envelope layout (one pod, in-process loop, plan
§2.3). Either way a **load** task then stages the produced envelopes and MERGEs them through
the landing table (``dl_products.cosmetics_rankings``) into the star (``dw_cosmetics``) in a
single BigQuery job (design §7).

Mode resolves from trigger params: any of ``backfill_start`` / ``backfill_end`` /
``backfill_snapshots`` set ⇒ backfill branch, else ⇒ scrape branch. A guard refuses backfill
params on a scheduled run so a leaked default can never flip one. ``max_active_runs=1``
serializes every run of this DAG, so a manual backfill never overlaps a scheduled run
mid-MERGE (no pool needed — the two modes are one DAG).

Execution model per CLAUDE.md: KubernetesExecutor, one task = one pod, GCP via ADC (no
gcp_conn_id). The scrape pods orchestrate Cloud Run; the backfill and load pods orchestrate
GCS/BigQuery I/O; the heavy compute runs off-pod.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, Variable, get_current_context, task
# pyrefly: ignore [missing-import]
from airflow.exceptions import AirflowSkipException
# pyrefly: ignore [missing-import]
from pendulum import datetime

try:
    # pyrefly: ignore [missing-import]
    from airflow.sdk import Param
except ImportError:  # pragma: no cover - import location varies across 3.x
    # pyrefly: ignore [missing-import]
    from airflow.models.param import Param

from external_data.common import cosmetics_backfill as cb
from external_data.common.cosmetics_bigquery import (
    cosmetics_staging_ndjson,
    run_cosmetics_transform,
)
from external_data.common.cosmetics_schema import RECIPES, recipe_source
from external_data.common.gcs_object import upload_replacing_object
from external_data.common.scrape_request import (
    build_request_payload,
    gcs_uri,
    request_object_name,
    result_object_name,
)
from external_data.common.scraper_cloud_run import (
    SCRAPE_EXECUTION_TIMEOUT,
    execute_scraper_job,
)


DAG_ID = "external_data__cosmetics"

_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")
_JOB_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")

# TEMPORARY: oliveyoung's Cloudflare challenge now blocks the Cloud Run egress IP on
# essentially every attempt (see engine.cosmetics.oliveyoung); the scraper never actually
# solves a served challenge, it only passed before when Cloudflare didn't challenge at all.
# Drop it from the live scrape fan-out until the egress/fingerprint issue is fixed so the
# other four recipes keep loading instead of the run needing manual salvage every day.
# RECIPES itself is untouched (backfill/normalize still handles oliveyoung snapshots).
SCRAPE_RECIPES = tuple(r for r in RECIPES if recipe_source(r) != "oliveyoung")


# --- config helpers (mirror estat_file_pipeline; GCP via ADC) --------------


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
    # Shares the materials star's GCP project/region; dedicated cosmetics datasets
    # (provisioned in deepfl-infra, tables owned by this DAG). Landing default dl_products,
    # star default dw_cosmetics.
    return {
        "project_id": _required_variable("materials_bigquery_project_id"),
        "region": _required_variable("materials_bigquery_region"),
        "landing_dataset_id": Variable.get("cosmetics_landing_dataset_id", default="dl_products"),
        "star_dataset_id": Variable.get("cosmetics_bigquery_dataset_id", default="dw_cosmetics"),
    }


def _label_value(value: Any) -> str:
    return _LABEL_DISALLOWED.sub("_", str(value).lower())[:63]


# --- mode resolution (pure; unit-tested) -----------------------------------


def backfill_requested(params: dict[str, Any]) -> bool:
    """True when any backfill trigger param is set (window or explicit snapshot list)."""
    return bool(
        params.get("backfill_start")
        or params.get("backfill_end")
        or params.get("backfill_snapshots")
    )


def resolve_mode_value(params: dict[str, Any], run_type: str | None) -> str:
    """'backfill_snapshots' or 'scrape_recipe', guarding scheduled runs against backfill.

    A scheduled run must never carry backfill params (a leaked default could otherwise flip
    it into a bulk historical replay), so that combination fails loudly.
    """
    if backfill_requested(params):
        if run_type == "scheduled":
            raise ValueError(
                "backfill params are not allowed on a scheduled run; trigger a manual run"
            )
        return "backfill_snapshots"
    return "scrape_recipe"


def selected_recipes(params: dict[str, Any]) -> list[str]:
    """Scrape-mode recipe subset (``recipes`` param) or all five; validates membership."""
    subset = params.get("recipes") or []
    if not subset:
        return list(RECIPES)
    unknown = [r for r in subset if r not in RECIPES]
    if unknown:
        raise ValueError(f"unknown recipes {unknown}; valid: {list(RECIPES)}")
    return [r for r in RECIPES if r in subset]


def selected_sources(params: dict[str, Any]) -> list[str]:
    """Backfill-mode source subset (``backfill_sources`` param) or all five sources."""
    subset = params.get("backfill_sources") or []
    valid = list(cb.BACKFILL_SOURCES)
    if not subset:
        return valid
    unknown = [s for s in subset if s not in valid]
    if unknown:
        raise ValueError(f"unknown backfill_sources {unknown}; valid: {valid}")
    return [s for s in valid if s in subset]


def snapshot_id_from_result_object(object_name: str) -> str:
    """Degenerate snapshot lineage from a result object path.

    ``scrape/results/{run_id}/{recipe}.json`` -> ``{run_id}`` (live);
    ``scrape/results/cnp_backfill/{snap}/{recipe}.json`` -> ``cnp_backfill/{snap}`` (backfill).
    """
    m = re.match(r"^scrape/results/(?P<mid>.+)/[^/]+\.json$", object_name)
    if not m:
        raise ValueError(f"unexpected result object path: {object_name!r}")
    return m.group("mid")


def _staging_object_name(run_id: str, snapshot_id: str, recipe: str) -> str:
    # Run-scoped staging so concurrent/historic loads never collide; the transform reads
    # the whole run's staged rows through one wildcard external table.
    safe = _JOB_ID_DISALLOWED.sub("_", f"{snapshot_id}_{recipe}")
    return f"scrape/staging/cosmetics/{run_id}/{safe}.ndjson"


def _resolve_backfill_snapshots(params: dict[str, Any], storage_client: Any, bucket: str, raw_prefix: str) -> list[str]:
    """Snapshot refs (``YYYYMMDD/THHMMSS``) for the backfill: explicit list or window scan.

    An explicit ``backfill_snapshots`` wins; otherwise list the snapshot "directories" under
    ``raw_prefix`` and keep those whose date falls in the inclusive ``[start, end]`` window.
    """
    explicit = params.get("backfill_snapshots") or []
    if explicit:
        return [cb.parse_snapshot_id(s) and f"{s.strip().strip('/')}" for s in explicit]

    start = (params.get("backfill_start") or "").replace("-", "")
    end = (params.get("backfill_end") or "").replace("-", "")
    snapshots: list[str] = []
    # Two-level delimiter listing: {raw_prefix}/{YYYYMMDD}/ then .../T{HHMMSS}/.
    date_prefixes = storage_client.list_blobs(
        bucket, prefix=f"{raw_prefix.strip('/')}/", delimiter="/"
    )
    list(date_prefixes)  # exhaust to populate .prefixes
    for date_prefix in sorted(getattr(date_prefixes, "prefixes", []) or []):
        date = date_prefix.rstrip("/").rsplit("/", 1)[-1]
        if not re.fullmatch(r"\d{8}", date):
            continue
        if (start and date < start) or (end and date > end):
            continue
        time_iter = storage_client.list_blobs(bucket, prefix=date_prefix, delimiter="/")
        list(time_iter)
        for time_prefix in sorted(getattr(time_iter, "prefixes", []) or []):
            tseg = time_prefix.rstrip("/").rsplit("/", 1)[-1]
            if re.fullmatch(r"T\d{6}", tseg):
                snapshots.append(f"{date}/{tseg}")
    return snapshots


with DAG(
    dag_id=DAG_ID,
    description=(
        "Cosmetics best-rankings: daily Cloud Run scrape (5 recipes) or manual historical "
        "backfill of cnp-scraping-raw snapshots -> GCS envelopes -> BigQuery landing "
        "(dl_products) + star (dw_cosmetics)."
    ),
    schedule="0 21 * * *",  # daily, evening KST (post business-day snapshot)
    start_date=datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # serializes scrape vs backfill; no BQ pool needed
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    params={
        # -- backfill trigger form (any set => backfill mode, manual runs only)
        "backfill_start": Param(None, type=["null", "string"], format="date",
                                title="Backfill start (YYYY-MM-DD, inclusive)"),
        "backfill_end": Param(None, type=["null", "string"], format="date",
                              title="Backfill end (YYYY-MM-DD, inclusive)"),
        "backfill_snapshots": Param([], type="array",
                                    title="Explicit snapshots (['YYYYMMDD/THHMMSS', ...]); overrides window"),
        "backfill_sources": Param([], type="array",
                                  title="Source subset (lottedfs/oliveyoung/ssgdfs/superpoint/naverbest); empty = all present"),
        "raw_bucket": Param(None, type=["null", "string"],
                            title="Raw bucket (default: scraper_scrape_bucket_name Variable)"),
        "raw_prefix": Param("cnp-scraping-raw", type="string", title="Raw prefix"),
        # -- scrape-mode knobs
        "recipes": Param([], type="array", title="Recipe subset rerun (flaky-site retries); empty = all five"),
        "max_details": Param(None, type=["null", "integer"], title="Detail-pass cap passthrough"),
    },
    tags=["external_data", "scraper", "cosmetics", "cloud-run", "bigquery"],
) as dag:

    @task.branch
    def resolve_mode() -> str:
        context = get_current_context()
        params = context["params"]
        run_type = getattr(context.get("dag_run"), "run_type", None)
        return resolve_mode_value(params, run_type)

    @task(execution_timeout=SCRAPE_EXECUTION_TIMEOUT)
    def scrape_recipe(recipe: str) -> dict[str, Any]:
        """Upload one recipe's request object and run the Cloud Run scraper job.

        Request-build folded in (5 pods per run, not build + fan-out; plan §2.2). A recipe
        outside the ``recipes`` subset skips, as does any run carrying backfill params
        (belt-and-suspenders on top of the branch: a backfill must never trigger live
        scrapes). No ``extra_env`` — cosmetics needs no secrets.
        """
        context = get_current_context()
        params = context["params"]
        if backfill_requested(params):
            raise AirflowSkipException("backfill run; live scrape does not apply")
        if recipe not in selected_recipes(params):
            raise AirflowSkipException(f"{recipe} not in requested recipes subset")

        run_id = context["run_id"]
        config = _config()
        bucket = config["bucket"]

        recipe_params: dict[str, Any] = {
            "html_prefix": gcs_uri(bucket, f"scrape/cosmetics_html/{run_id}/{recipe}"),
        }
        if params.get("max_details") is not None:
            recipe_params["max_details"] = params["max_details"]

        request = build_request_payload(recipe, recipe_params)
        request_object = request_object_name(run_id, recipe)
        output_uri = gcs_uri(bucket, result_object_name(run_id, recipe))

        print(f"::group::[cosmetics] scrape {recipe} -> {output_uri}")
        from google.cloud import storage  # deferred; ADC

        import json as _json

        upload_replacing_object(
            storage.Client(),
            bucket_name=bucket,
            object_name=request_object,
            data=_json.dumps(request, ensure_ascii=False),
            mime_type="application/json",
        )
        result = execute_scraper_job(
            project_id=config["project_id"],
            region=config["region"],
            job_name=config["job_name"],
            request_uri=gcs_uri(bucket, request_object),
            output_uri=output_uri,
        )
        print(f"[cosmetics] recipe={recipe} execution={result.get('execution_name')} result={output_uri}")
        print("::endgroup::")
        return {"recipe": recipe, "result_uri": output_uri, **result}

    @task(execution_timeout=timedelta(hours=1))
    def backfill_snapshots() -> dict[str, Any]:
        """One pod normalizes every (snapshot, source) into a result envelope (plan §2.3–§2.7).

        Per-item isolation: missing/empty raw files are skipped with a reason in the report,
        one bad item never hides the rest, and the task fails once at the end if the operator's
        window resolved to nothing or wrote zero envelopes. Idempotent: envelopes are keyed by
        snapshot id, so a rerun overwrites the same objects.
        """
        from google.cloud import storage  # deferred; ADC

        context = get_current_context()
        params = context["params"]
        config = _config()
        client = storage.Client()

        raw_bucket = params.get("raw_bucket") or config["bucket"]
        raw_prefix = params.get("raw_prefix") or "cnp-scraping-raw"
        out_bucket = config["bucket"]
        sources = selected_sources(params)
        snapshots = _resolve_backfill_snapshots(params, client, raw_bucket, raw_prefix)

        report: list[dict[str, Any]] = []
        errors: list[str] = []
        envelopes_written = 0

        if not snapshots:
            raise ValueError(
                "backfill resolved zero snapshots; check backfill_start/backfill_end/"
                "backfill_snapshots and raw_prefix"
            )

        def _read(bucket_name: str, object_name: str) -> str | None:
            blob = client.bucket(bucket_name).blob(object_name)
            if not blob.exists():
                return None
            return blob.download_as_bytes().decode("utf-8-sig")

        for snapshot in snapshots:
            snap_prefix = cb.raw_snapshot_prefix(raw_prefix, snapshot)
            for source in sources:
                recipe = next(r for r in RECIPES if recipe_source(r) == source)
                item = {"snapshot": cb.parse_snapshot_id(snapshot), "source": source, "recipe": recipe}
                print(f"::group::[cosmetics-backfill] {snapshot} / {source}")
                try:
                    kind = cb.base_file_kind(source)
                    if kind == "csv":
                        text = None
                        for cand in cb.rankings_csv_candidates(source):
                            text = _read(raw_bucket, f"{snap_prefix}/{cand}")
                            if text:
                                item["base_file"] = cand
                                break
                        base_rows = cb.parse_csv(text) if text else []
                    else:
                        name = cb.base_ndjson_name(source)
                        text = _read(raw_bucket, f"{snap_prefix}/{name}")
                        item["base_file"] = name
                        base_rows = cb.parse_ndjson(text) if text else []

                    if not text:
                        item["status"] = "skipped"
                        item["reason"] = "rankings file absent"
                        print(f"[cosmetics-backfill] skip {source}: rankings file absent")
                        print("::endgroup::")
                        report.append(item)
                        continue
                    if not base_rows:
                        item["status"] = "skipped"
                        item["reason"] = "zero-row rankings (prototype failure)"
                        print(f"[cosmetics-backfill] skip {source}: zero rows")
                        print("::endgroup::")
                        report.append(item)
                        continue

                    detail_name = cb.detail_ndjson_name(source)
                    detail_rows = None
                    html_codes = None
                    if detail_name:
                        dtext = _read(raw_bucket, f"{snap_prefix}/{detail_name}")
                        detail_rows = cb.parse_ndjson(dtext) if dtext else None
                        if detail_rows:
                            # An interrupted prototype run can have detail rows whose HTML
                            # never got baked; one list call scopes html_object to pages
                            # that exist so no dead gs:// links land in the star.
                            html_prefix = f"{snap_prefix}/{source}_details/"
                            html_codes = {
                                b.name[len(html_prefix):-len(".html")]
                                for b in client.list_blobs(raw_bucket, prefix=html_prefix)
                                if b.name.endswith(".html")
                            }
                            item["html_pages"] = len(html_codes)

                    records = cb.normalize_records(
                        source,
                        base_rows=base_rows,
                        detail_rows=detail_rows,
                        snapshot=snapshot,
                        raw_bucket=raw_bucket,
                        raw_prefix=raw_prefix,
                        html_codes=html_codes,
                    )
                    envelope = cb.build_envelope(
                        recipe, snapshot=snapshot, records=records,
                        raw_bucket=raw_bucket, raw_prefix=raw_prefix,
                    )
                    import json as _json

                    object_name = cb.result_object_name(snapshot, recipe)
                    upload_replacing_object(
                        client,
                        bucket_name=out_bucket,
                        object_name=object_name,
                        data=_json.dumps(envelope, ensure_ascii=False),
                        mime_type="application/json",
                    )
                    envelopes_written += 1
                    item["status"] = "written"
                    item["rows"] = len(records)
                    item["enriched"] = sum(1 for r in records if r["html_object"])
                    item["result_uri"] = gcs_uri(out_bucket, object_name)
                    print(f"[cosmetics-backfill] wrote {item['rows']} rows -> {item['result_uri']}")
                except Exception as exc:  # noqa: BLE001 - isolate per item, fail once at end
                    item["status"] = "error"
                    item["reason"] = repr(exc)
                    errors.append(f"{snapshot}/{source}: {exc}")
                    print(f"[cosmetics-backfill] ERROR {source}: {exc!r}")
                print("::endgroup::")
                report.append(item)

        summary = {
            "snapshots": [cb.parse_snapshot_id(s) for s in snapshots],
            "sources": sources,
            "envelopes_written": envelopes_written,
            "errors": errors,
            "items": report,
        }
        print(f"[cosmetics-backfill] summary: {envelopes_written} envelopes, {len(errors)} errors")
        if errors:
            raise RuntimeError(f"backfill had {len(errors)} per-item error(s): {errors[:5]}")
        if envelopes_written == 0:
            raise ValueError("backfill wrote zero envelopes; operator window/prefix is wrong")
        return summary

    @task(trigger_rule="none_failed_min_one_success")
    def load_to_bigquery() -> dict[str, Any]:
        """Stage this run's envelopes and MERGE them landing -> star in one BigQuery job.

        Runs in either mode (downstream of both branches): scrape mode sweeps
        ``scrape/results/{run_id}/``, backfill mode sweeps the resolved
        ``scrape/results/cnp_backfill/{snap}/`` prefixes. Each envelope is flattened to
        lineage-stamped NDJSON under a run-scoped staging prefix, then one transform job reads
        the whole staging wildcard.
        """
        from google.cloud import bigquery, storage  # deferred; ADC

        context = get_current_context()
        params = context["params"]
        run_id = context["run_id"]
        run_type = getattr(context.get("dag_run"), "run_type", None)
        config = _config()
        bq = _bq_config()
        storage_client = storage.Client()
        bucket = config["bucket"]

        mode = resolve_mode_value(params, run_type)
        result_prefixes: list[str] = []
        if mode == "backfill_snapshots":
            raw_bucket = params.get("raw_bucket") or bucket
            raw_prefix = params.get("raw_prefix") or "cnp-scraping-raw"
            snaps = _resolve_backfill_snapshots(params, storage_client, raw_bucket, raw_prefix)
            for snap in snaps:
                result_prefixes.append(f"scrape/results/cnp_backfill/{cb.parse_snapshot_id(snap)}/")
        else:
            result_prefixes.append(f"scrape/results/{run_id}/")

        # Stage every envelope under the resolved prefixes into lineage-stamped NDJSON.
        import json as _json

        staged = 0
        staged_rows = 0
        for prefix in result_prefixes:
            for blob in storage_client.list_blobs(bucket, prefix=prefix):
                if not blob.name.endswith(".json"):
                    continue
                envelope = _json.loads(blob.download_as_bytes().decode("utf-8"))
                snapshot_id = snapshot_id_from_result_object(blob.name)
                ndjson = cosmetics_staging_ndjson(
                    envelope,
                    snapshot_id=snapshot_id,
                    envelope_uri=gcs_uri(bucket, blob.name),
                )
                if not ndjson:
                    continue
                recipe = blob.name.rsplit("/", 1)[-1][: -len(".json")]
                staging_object = _staging_object_name(run_id, snapshot_id, recipe)
                upload_replacing_object(
                    storage_client,
                    bucket_name=bucket,
                    object_name=staging_object,
                    data=ndjson,
                    mime_type="application/x-ndjson",
                )
                staged += 1
                staged_rows += ndjson.count("\n") + 1

        if staged == 0:
            raise ValueError(f"load found no non-empty envelopes under {result_prefixes}")

        raw_gcs_uri = gcs_uri(bucket, f"scrape/staging/cosmetics/{run_id}/*")
        labels = {
            "dag_id": _label_value(DAG_ID),
            "run_id": _label_value(run_id),
            "source": "cosmetics",
            "step": "load",
        }
        job_id_prefix = _JOB_ID_DISALLOWED.sub("_", f"cosmetics_{run_id}_load_")[:512]

        client = bigquery.Client(project=bq["project_id"], location=bq["region"])
        print(f"::group::[cosmetics] load {staged} envelopes ({staged_rows} rows) -> star")
        result = run_cosmetics_transform(
            client,
            project_id=bq["project_id"],
            landing_dataset_id=bq["landing_dataset_id"],
            star_dataset_id=bq["star_dataset_id"],
            region=bq["region"],
            raw_gcs_uri=raw_gcs_uri,
            expected_row_count=None,
            labels=labels,
            job_id_prefix=job_id_prefix,
        )
        print(
            f"[cosmetics] load merged={result.get('dml_affected_rows')} "
            f"job_id={result.get('job_id')} statements={len(result.get('statements') or [])}"
        )
        print("::endgroup::")
        return {"mode": mode, "staged_envelopes": staged, "staged_rows": staged_rows, "transform": result}

    branch = resolve_mode()
    scrape = scrape_recipe.expand(recipe=list(SCRAPE_RECIPES))  # oliveyoung excluded, see SCRAPE_RECIPES above
    backfill = backfill_snapshots()
    load = load_to_bigquery()

    branch >> [scrape, backfill] >> load
