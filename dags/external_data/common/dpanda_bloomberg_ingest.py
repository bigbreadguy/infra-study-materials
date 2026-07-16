"""Shared ingest engine for the dpanda Bloomberg pipelines (-> dl_materials).

Both the scheduled DAG (NDJSON, D-1 single day) and the manual backfill DAG
(Parquet, explicit window) call :func:`ingest`. It extracts every grain target
for the window over one Mongo session, reshapes each sample into the dl_materials
``{"row": {...}}`` record shape (one flat object per Mongo document: grain_id, a
normalized ``logical_date``, and the document's ``data`` fields), uploads one raw
object per grain under a run-scoped GCS prefix, then transforms every grain in a
SINGLE BigQuery job through the shared materials transform: one multi-statement
script reads all of the run's objects through a job-scoped temporary external
table (a run-scoped wildcard) and bootstraps + merges dim_datasources,
dim_categories, dim_metrics, and fact_values for every metric at once.

Bloomberg is modeled as one dl_materials datasource (``dpanda_bloomberg``); each
category (copper/nickel/steel/aluminum/market_macro) is a dl_materials category, and each
grain x field is a dl_materials metric. There is no longer a dim_grains table or
OHLCV-explosion merge: the per-category materials mapping config
(``configs/materials_metrics/dpanda_bloomberg.<category>.json``) names, for each
grain, which ``data`` field to read (``measure_column``, selected by a
``match: {"grain_id": ...}`` predicate), its curated metric name/description/unit,
its category, and its time grain. The config is the single source of truth for
what loads; the extractor is config-agnostic and just flattens every document.

The extract targets the previous completed UTC day (D-1) for the scheduled path;
the backfill names an explicit half-open UTC window. Both DAGs write the same
dl_materials tables, so every task takes the shared size-1 ``dpanda_bloomberg``
pool, which serializes writes across all scheduled DAGs and the backfill so a
backfill never overlaps a scheduled run mid-MERGE. The pool must be provisioned
in the deployment.

Required Airflow Variables (all namespaced dpanda_):
- dpanda_mongo_conn_id, dpanda_mongo_database_name, dpanda_mongo_collection_name
- dpanda_gcs_bucket_name, dpanda_gcs_raw_prefix
- dpanda_bigquery_project_id, dpanda_bigquery_dataset_id, dpanda_bigquery_region

All GCP access runs as the Airflow workload identity service account through
Application Default Credentials; per dev policy tasks use no impersonation chains.
"""

from __future__ import annotations

import io
import re
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any

# pyrefly: ignore [missing-import]
import pendulum

from external_data.common.gcs_object import upload_replacing_object
from external_data.common.materials_bigquery import run_materials_transform
from external_data.common.materials_metrics import (
    grain_targets_from_config,
    load_materials_config,
)


# One DAG per category; the scheduled file builds these, the backfill DAG
# validates its category Param against them.
CATEGORIES = {
    "copper": {"schedule": "@daily"},
    "nickel": {"schedule": "@daily"},
    "market_macro": {"schedule": "@daily"},
    "steel": {"schedule": "@daily"},
    "aluminum": {"schedule": "@daily"},
}

# A single size-1 pool shared by every scheduled DAG and the backfill DAG. It
# serializes writes to the dl_materials tables across all of them so a manual
# backfill never overlaps a scheduled run mid-MERGE. The backfill's category is a
# runtime Param, so the lock cannot be per-category (a task's pool is fixed at
# parse time); one global pool is the price of a single static lock. Must be
# provisioned in the deployment: `airflow pools set dpanda_bloomberg 1 "..."`.
POOL_NAME = "dpanda_bloomberg"

# All Bloomberg categories share one dl_materials datasource; the category JSON
# files are recipes named "<DATASOURCE>.<category>".
MATERIALS_DATASOURCE = "dpanda_bloomberg"

# Truncate the logged sample so a single grain's raw line cannot flood the log.
SAMPLE_PREVIEW_CHARS = 280

# Raw object format -> file extension, upload mime type, and BigQuery source
# format. NDJSON keeps ``row`` as JSON; Parquet stores it as a STRING column.
_RAW_FORMATS = {
    "ndjson": {"ext": "ndjson", "mime": "application/x-ndjson",
               "source_format": "NEWLINE_DELIMITED_JSON"},
    "parquet": {"ext": "parquet", "mime": "application/octet-stream",
                "source_format": "PARQUET"},
}

# Mongo ``data`` keys that carry the observation date, tried in order before the
# document's top-level ``ts``.
_DATE_KEYS = ("dt", "date", "Date", "DATE")

# Canonical market-measure name -> the source ``data`` aliases that may carry it,
# in preference order. The extractor normalizes each present alias into its
# canonical lowercase key so a materials config references one stable field name
# (open/high/low/close/volume/openinterest/value) regardless of source casing --
# mirroring the COALESCE-over-aliases the old per-grain OHLCV merge did in SQL.
_MEASURE_ALIASES = {
    "open": ("open", "Open"),
    "high": ("high", "High"),
    "low": ("low", "Low"),
    "close": ("close", "Close"),
    "volume": ("volume", "Volume"),
    "openinterest": ("oi", "openInterest", "OpenInterest"),
    "value": ("value", "Value"),
}

# BigQuery labels accept only [a-z0-9_-] capped at 63 chars; job-id prefixes
# allow mixed case and are far longer. Slugify free-form identity (run ids carry
# colons and plus signs) so the job still submits.
_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")
_JOB_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")


def materials_recipe(category: str) -> str:
    """The materials mapping recipe name for a Bloomberg category."""
    return f"{MATERIALS_DATASOURCE}.{category}"


def _utc_now_iso() -> str:
    return pendulum.now("UTC").to_iso8601_string()


def _label_value(value: Any) -> str:
    return _LABEL_DISALLOWED.sub("_", str(value).lower())[:63]


def _run_token(run_id: str) -> str:
    # The run-scoped GCS prefix needs a filesystem-safe, collision-free segment;
    # the raw run id carries ':' and '+' from scheduled/triggered run ids.
    return _JOB_ID_DISALLOWED.sub("_", str(run_id))[:128]


@contextmanager
def _log_group(title: str):
    # GitHub-Actions-style markers the Airflow log viewer folds into a
    # collapsible section, restoring per-step visibility lost when the per-grain
    # task chain collapsed into one task.
    print(f"::group::{title}")
    try:
        yield
    finally:
        print("::endgroup::")


def _require_variable(name: str, value: Any) -> Any:
    if not value:
        raise ValueError(f"Airflow Variable '{name}' must be set")
    return value


def _required_airflow_variable(name: str) -> str:
    # pyrefly: ignore [missing-import]
    from airflow.sdk import Variable

    return _require_variable(name, Variable.get(name, default=""))


def _load_ingestion_config() -> dict[str, Any]:
    return {
        "mongo_conn_id": _required_airflow_variable("dpanda_mongo_conn_id"),
        "mongo_database_name": _required_airflow_variable(
            "dpanda_mongo_database_name"
        ),
        "mongo_collection_name": _required_airflow_variable(
            "dpanda_mongo_collection_name"
        ),
        "gcs_bucket_name": _required_airflow_variable("dpanda_gcs_bucket_name"),
        "gcs_raw_prefix": _required_airflow_variable("dpanda_gcs_raw_prefix"),
        "bigquery_project_id": _required_airflow_variable(
            "dpanda_bigquery_project_id"
        ),
        "bigquery_dataset_id": _required_airflow_variable(
            "dpanda_bigquery_dataset_id"
        ),
        "bigquery_region": _required_airflow_variable("dpanda_bigquery_region"),
    }


def _window_token(window_start) -> str:
    return window_start.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _run_scoped_prefix(raw_prefix, category, run_id, window_start) -> str:
    # One wildcard over this prefix captures exactly this window's objects: the
    # category segment isolates concurrent categories, the run segment isolates
    # reruns, and the window segment isolates each backfill year-chunk (which
    # share a run id) so one combined job never reads another chunk's data.
    parts = [
        raw_prefix.strip("/"),
        f"category={category}",
        f"run={_run_token(run_id)}",
        f"window={_window_token(window_start)}",
    ]
    return "/".join(part for part in parts if part)


def _build_gcs_object_name(run_prefix, logical_date, dataset_id, grain_id, ext):
    logical_date = logical_date.astimezone(timezone.utc)
    logical_date_path = logical_date.strftime("%Y/%m/%d/%H")
    logical_date_token = logical_date.strftime("%Y%m%dT%H%M%S%z")
    # Grain identity is the dataset id and grain id pair: the same grain_id
    # recurs under several datasets, so a grain_id-only path would overwrite a
    # sibling's object for the same interval.
    parts = [
        run_prefix,
        f"dataset_id={dataset_id}",
        f"grain_id={grain_id}",
        logical_date_path,
        f"raw-{logical_date_token}.{ext}",
    ]
    return "/".join(part for part in parts if part)


def _run_wildcard_uri(bucket_name: str, run_prefix: str) -> str:
    return f"gs://{bucket_name}/{run_prefix}/*"


def _point_probe_missing(collection, dataset_object_id, grain_id) -> bool:
    # An indexed point probe separates a wrong dataset id to grain id mapping,
    # which must fail loudly, from a day with no data, a legitimate quiet day.
    return collection.find_one(
        {"datasetId": dataset_object_id, "grainId": grain_id}, {"_id": 1}
    ) is None


def _extract_grain_docs(collection, target, start, end, category):
    # pyrefly: ignore [missing-import]
    from bson import ObjectId

    grain_id = target["grain_id"]
    dataset_object_id = ObjectId(target["dataset_id"])

    if _point_probe_missing(collection, dataset_object_id, grain_id):
        raise ValueError(
            f"No documents match datasetId {target['dataset_id']} with "
            f"grainId {grain_id}; check materials metrics file "
            f"{materials_recipe(category)}.json"
        )

    # Lead with datasetId so the find stays on the compound index over
    # datasetId, grainId, and ts.
    return list(
        collection.find({
            "datasetId": dataset_object_id,
            "grainId": grain_id,
            "ts": {"$gte": start, "$lt": end},
        })
    )


def _normalize_date(value: Any) -> str | None:
    """A source value -> 'YYYY-MM-DD', or None when it cannot be resolved.

    The materials merge parses ``row.logical_date`` with PARSE_DATE('%Y-%m-%d'),
    so the extractor pins every observation date into that exact form here rather
    than leaving date parsing to SQL.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        # BSON dates are UTC; pymongo returns them naive by default, so a naive
        # value is already UTC -- do NOT astimezone() it (that would apply the
        # pod's local offset and shift the date). Aware values convert to UTC.
        if value.tzinfo is None:
            return value.date().isoformat()
        return value.astimezone(timezone.utc).date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    parsed = pendulum.parse(text, strict=False)
    if isinstance(parsed, (datetime, pendulum.DateTime)):
        return parsed.in_timezone("UTC").date().isoformat()
    return str(parsed)[:10]


def _row_logical_date(doc: dict) -> str | None:
    data = doc.get("data")
    if isinstance(data, dict):
        for key in _DATE_KEYS:
            resolved = _normalize_date(data.get(key))
            if resolved:
                return resolved
    return _normalize_date(doc.get("ts"))


def _materials_row(doc: dict, target: dict) -> dict[str, Any]:
    """One flat dl_materials ``row`` object for a single Mongo document.

    Carries the grain id (so the config's ``match: {"grain_id": ...}`` selects
    it), the normalized observation date, and every ``data`` field flattened to
    the top level (so a metric's ``measure_column`` names a data key directly).
    Config-agnostic: the per-category mapping decides which fields become metrics.
    """
    row: dict[str, Any] = {
        "grain_id": target["grain_id"],
        "dataset_id": target["dataset_id"],
        "logical_date": _row_logical_date(doc),
    }
    data = doc.get("data")
    if isinstance(data, dict):
        for key, value in data.items():
            # Never let a data field shadow the identity/date columns the config
            # and merge rely on.
            if key not in row:
                row[key] = value
        # Add canonical OHLCV keys from whichever alias the source used, so the
        # config's measure_column can always name the lowercase canonical field.
        for canonical, aliases in _MEASURE_ALIASES.items():
            if row.get(canonical) not in (None, ""):
                continue
            for alias in aliases:
                value = data.get(alias)
                if value not in (None, ""):
                    row[canonical] = value
                    break
    return row


def _serialize_grain(docs, target, raw_format):
    """Raw payload + a truncated sample for one grain, in the chosen format."""
    # pyrefly: ignore [missing-import]
    from bson import json_util

    rows = [_materials_row(doc, target) for doc in docs]
    if raw_format == "parquet":
        payload = _grain_parquet_bytes(rows)
        sample = json_util.dumps(rows[0]) if rows else ""
    else:
        lines = [json_util.dumps({"row": row}) for row in rows]
        payload = "\n".join(lines) + "\n"
        sample = lines[0] if lines else ""
    sample = str(sample)[:SAMPLE_PREVIEW_CHARS]
    return payload, sample


def _grain_parquet_bytes(rows) -> bytes:
    # pyrefly: ignore [missing-import]
    import pyarrow as pa
    # pyrefly: ignore [missing-import]
    import pyarrow.parquet as pq
    # pyrefly: ignore [missing-import]
    from bson import json_util

    # One STRING column "row" holding each record's JSON text; the Parquet
    # external table reads it as STRING and JSON_VALUE parses it identically to
    # the NDJSON JSON column.
    schema = pa.schema([("row", pa.string())])
    table = pa.table({"row": [json_util.dumps(row) for row in rows]}, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _extract_raw_data_to_gcs(targets, start, end, category, run_id, raw_format):
    """Extract the window for every grain over one Mongo session.

    Returns the run-scoped wildcard uri, the per-grain raw locations (for
    observability), the total document count (the validate ASSERT target), and
    the quiet grains. A grain with no samples is a legitimate quiet day: it
    uploads nothing, so the wildcard captures only grains with data. One bad
    grain (wrong mapping, failed upload) fails the whole extract; retries reuse
    the same run-scoped prefix and replace the objects, staying idempotent.
    """
    # pyrefly: ignore [missing-import]
    from airflow.providers.mongo.hooks.mongo import MongoHook
    # pyrefly: ignore [missing-import]
    from google.cloud import storage

    config = _load_ingestion_config()
    bucket_name = config["gcs_bucket_name"]
    run_prefix = _run_scoped_prefix(
        config["gcs_raw_prefix"], category, run_id, start
    )
    file_format = _RAW_FORMATS[raw_format]
    print(f"Extracting {category} interval {start} to {end} as {raw_format}")

    raw_locations = []
    quiet_grains = []
    total_documents = 0
    gcs_client = storage.Client()

    with MongoHook(mongo_conn_id=config["mongo_conn_id"]) as hook:
        collection = (
            hook.get_conn()
            .get_database(config["mongo_database_name"])
            .get_collection(config["mongo_collection_name"])
        )

        for target in targets:
            grain_label = f"{target['grain_id']}@{target['dataset_id']}"
            with _log_group(f"extract · {grain_label}"):
                started = time.monotonic()
                print(f"[{_utc_now_iso()}] extract start {grain_label}")
                docs = _extract_grain_docs(
                    collection, target, start, end, category
                )

                if not docs:
                    quiet_grains.append(grain_label)
                    print(
                        f"[{_utc_now_iso()}] extract done {grain_label}: "
                        "0 documents (quiet grain)"
                    )
                    continue

                payload, sample = _serialize_grain(docs, target, raw_format)
                object_name = _build_gcs_object_name(
                    run_prefix,
                    start,
                    target["dataset_id"],
                    target["grain_id"],
                    file_format["ext"],
                )
                uploaded_object_name = upload_replacing_object(
                    gcs_client,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=payload,
                    mime_type=file_format["mime"],
                )
                total_documents += len(docs)
                print(f"raw docs: {len(docs)}  sample: {sample}")
                print(
                    f"[{_utc_now_iso()}] extract done {grain_label}: "
                    f"uploaded {len(docs)} documents to "
                    f"gs://{bucket_name}/{uploaded_object_name} in "
                    f"{round(time.monotonic() - started, 3)}s"
                )
                raw_locations.append({
                    "bucket": bucket_name,
                    "object": uploaded_object_name,
                    "document_count": len(docs),
                    "sample": sample,
                    "dataset_id": target["dataset_id"],
                    "grain_id": target["grain_id"],
                    "grain_description": target["description"],
                    "time_grain": target["freq"],
                })

    if quiet_grains:
        print(
            f"No samples between {start} and {end} for {len(quiet_grains)} "
            f"quiet grains: {', '.join(quiet_grains)}"
        )
    print(
        f"Extracted {len(raw_locations)} grain objects "
        f"({total_documents} documents) out of {len(targets)} targets"
    )
    return {
        "raw_wildcard_uri": _run_wildcard_uri(bucket_name, run_prefix),
        "raw_locations": raw_locations,
        "total_documents": total_documents,
        "quiet_grains": quiet_grains,
    }


def _transform_labels(run_meta):
    return {
        "dag_id": _label_value(run_meta["dag_id"]),
        "run_id": _label_value(run_meta["run_id"]),
        "category": _label_value(run_meta["category"]),
        "step": "transform",
    }


def _transform_job_id_prefix(run_meta):
    raw = (
        f"dpanda_{run_meta['category']}_{_run_token(run_meta['run_id'])}_"
        "transform_"
    )
    return _JOB_ID_DISALLOWED.sub("_", raw)[:512]


def _bigquery_client(config):
    # pyrefly: ignore [missing-import]
    from google.cloud import bigquery

    return bigquery.Client(
        project=config["bigquery_project_id"],
        location=config["bigquery_region"],
    )


def _load_materials_config(category: str):
    recipe = materials_recipe(category)
    materials_config = load_materials_config(recipe)
    if materials_config is None:
        raise ValueError(
            f"No materials mapping config for {recipe}; add "
            f"configs/materials_metrics/dpanda_bloomberg/{recipe}.json"
        )
    return materials_config


def _metric_grain_index(materials_config) -> dict[str, tuple[str, str]]:
    """Map each grain-based curated metric name to its ``(dataset_id, grain_id)``.

    Built from the parsed (post-expansion) config metrics, so both single-field
    metrics (``Com_LME_Cu_Cash``) and OHLCV-expanded field metrics
    (``Com_SHFE_Cu_open``) resolve. Several metric names may share one grain (the
    OHLCV expansion); the surgical filter dedupes on the pair downstream.
    """
    index: dict[str, tuple[str, str]] = {}
    for metric in materials_config["metrics"]:
        match = metric.get("match") or {}
        grain_id = match.get("grain_id")
        dataset_id = match.get("dataset_id")
        if grain_id and dataset_id:
            index[metric["name"]] = (dataset_id, grain_id)
    return index


def _select_targets_for_metrics(targets, materials_config, metric_names, category):
    """Filter extraction grains down to those backing the named metrics.

    Resolves each curated metric name to its grain via the config, then keeps only
    the matching ``(dataset_id, grain_id)`` targets so the extract pulls just those
    grains from Mongo. Unknown names fail loudly with the valid set, so a typo in
    the trigger never silently extracts nothing.
    """
    index = _metric_grain_index(materials_config)
    requested = list(dict.fromkeys(name.strip() for name in metric_names if name and name.strip()))
    if not requested:
        return targets
    unknown = [name for name in requested if name not in index]
    if unknown:
        raise ValueError(
            f"Unknown metric name(s) {sorted(unknown)} for category {category!r}; "
            f"valid grain-based metrics: {sorted(index)}"
        )
    wanted_pairs = {index[name] for name in requested}
    selected = [
        target
        for target in targets
        if (target["dataset_id"], target["grain_id"]) in wanted_pairs
    ]
    print(
        f"Surgical filter: {len(requested)} metric(s) -> {len(selected)} grain(s) "
        f"of {len(targets)} in {category}: {', '.join(requested)}"
    )
    return selected


def _run_transform(extract_result, run_meta, raw_format, materials_config):
    """Transform every grain in one BigQuery job and report per-step stats."""
    config = _load_ingestion_config()
    client = _bigquery_client(config)

    started = time.monotonic()
    with _log_group(f"transform · {run_meta['category']}"):
        print(
            f"[{_utc_now_iso()}] transform start over "
            f"{extract_result['raw_wildcard_uri']} "
            f"({extract_result['total_documents']} raw docs, "
            f"{len(extract_result['raw_locations'])} grains)"
        )
        result = run_materials_transform(
            client,
            project_id=config["bigquery_project_id"],
            dataset_id=config["bigquery_dataset_id"],
            region=config["bigquery_region"],
            raw_gcs_uri=extract_result["raw_wildcard_uri"],
            config=materials_config,
            expected_row_count=extract_result["total_documents"],
            source_format=_RAW_FORMATS[raw_format]["source_format"],
            labels=_transform_labels(run_meta),
            job_id_prefix=_transform_job_id_prefix(run_meta),
        )
        elapsed_s = round(time.monotonic() - started, 3)
        print(
            f"[{_utc_now_iso()}] transform done job {result['job_id']} "
            f"({elapsed_s}s); terminal DML rows: "
            f"{result.get('dml_affected_rows')}"
        )
    result["elapsed_s"] = elapsed_s
    return result


def _format_transform_summary(extract_result, transform_result):
    lines = ["Run summary:"]
    lines.append(
        f"  grains with data: {len(extract_result['raw_locations'])}  "
        f"quiet grains: {len(extract_result['quiet_grains'])}  "
        f"raw docs: {extract_result['total_documents']}"
    )
    lines.append(
        f"  transform job: {transform_result.get('job_id')} "
        f"({transform_result.get('elapsed_s')}s)"
    )
    statements = transform_result.get("statements") or []
    if statements:
        lines.append("  statements (in order):")
        for index, statement in enumerate(statements):
            rows = statement.get("dml_affected_rows")
            lines.append(
                f"    {index:>2} {statement.get('statement_type') or '?':<14} "
                f"job={statement.get('job_id')} "
                f"rows={'n/a' if rows is None else rows}"
            )
    else:
        lines.append(
            "  statements: none reported (single-statement client or fake)"
        )
    return "\n".join(lines)


def ingest(*, category, window, raw_format="ndjson", context, metric_names=None):
    """Extract every grain and transform them in one pod, one BigQuery job.

    ``window`` is the half-open ``(start, end)`` UTC interval to ingest. The
    extract pays the Mongo/Atlas fixed costs once; the transform is a single
    BigQuery job over the run-scoped wildcard, driven by the category's materials
    mapping config. Returns a structured XCom report.

    ``metric_names`` optionally restricts the extract to the grains backing those
    curated metrics (surgical backfill): only their grains are pulled from Mongo
    and uploaded, so a long backfill of a handful of daily series no longer drags
    the whole category's grains through every chunk. The transform config stays
    whole -- metrics whose grains were not extracted simply contribute no merge
    candidates and are untouched. ``None``/empty backfills the whole category.
    """
    if category not in CATEGORIES:
        raise ValueError(
            f"Unknown category {category!r}; expected one of "
            f"{sorted(CATEGORIES)}"
        )
    if raw_format not in _RAW_FORMATS:
        raise ValueError(
            f"Unknown raw_format {raw_format!r}; expected one of "
            f"{sorted(_RAW_FORMATS)}"
        )

    materials_config = _load_materials_config(category)
    start, end = window
    run_started = time.monotonic()
    # dag_run is reliably present in the task context across Airflow versions
    # and carries both the dag id and run id used for labels and the run-scoped
    # GCS prefix.
    dag_run = context["dag_run"]
    run_meta = {
        "dag_id": str(dag_run.dag_id),
        "run_id": str(getattr(dag_run, "run_id", context.get("run_id"))),
        "category": category,
    }
    print(f"[{_utc_now_iso()}] ingest run start for {category}")

    targets = grain_targets_from_config(materials_recipe(category))
    if metric_names:
        targets = _select_targets_for_metrics(
            targets, materials_config, metric_names, category
        )

    extract_result = _extract_raw_data_to_gcs(
        targets,
        start,
        end,
        category,
        run_meta["run_id"],
        raw_format,
    )

    if not extract_result["raw_locations"]:
        # Most daily runs are legitimately quiet: week, month, quarter, and
        # year grains only publish on their period boundary, so an all-empty
        # interval is normal. Succeed silently without a warning.
        print(
            f"No grains had samples for {start} to {end}; nothing to transform"
        )
        print(
            f"[{_utc_now_iso()}] ingest run done for {category} in "
            f"{round(time.monotonic() - run_started, 3)}s (quiet)"
        )
        return {"run": run_meta, "extract": extract_result, "transform": None}

    transform_result = _run_transform(
        extract_result, run_meta, raw_format, materials_config
    )
    print(_format_transform_summary(extract_result, transform_result))
    print(
        f"[{_utc_now_iso()}] ingest run done for {category} in "
        f"{round(time.monotonic() - run_started, 3)}s"
    )
    return {
        "run": run_meta,
        "extract": {
            "raw_wildcard_uri": extract_result["raw_wildcard_uri"],
            "total_documents": extract_result["total_documents"],
            "grains_with_data": len(extract_result["raw_locations"]),
            "quiet_grains": extract_result["quiet_grains"],
            "raw_locations": extract_result["raw_locations"],
        },
        "transform": transform_result,
    }


def backfill_window_chunks(start, end):
    """Split a half-open [start, end) UTC window into per-calendar-year chunks.

    fact_values is yearly-partitioned, so the merge no longer needs chunking to
    stay under a partition cap (a multi-decade MERGE touches one partition per
    year). Chunking is kept for the extract side: each chunk pulls only one
    year of Mongo documents into pod memory and uploads one bounded raw object
    set, and each chunk is an independently idempotent extract + combined
    BigQuery job, so a failed year replays without redoing the whole window. The
    first chunk may be a partial year; the rest align to calendar-year
    boundaries.
    """
    if end <= start:
        raise ValueError(f"Backfill end {end} must be after start {start}")
    chunks = []
    cursor = start
    while cursor < end:
        next_year = cursor.start_of("year").add(years=1)
        chunk_end = min(next_year, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def run_backfill(*, category, window, context, metric_names=None):
    """Ingest a long [start, end) window as a sequence of per-year chunks.

    One combined BigQuery job per year bounds per-chunk pod memory and gives
    independent per-year idempotent replay (the table is yearly-partitioned, so
    chunking is no longer required by any partition cap). The whole loop runs in
    one task holding the shared pool start to finish, so
    a scheduled run for any category waits behind the backfill (accepted) rather
    than overlapping it. Returns a per-chunk report list.

    ``metric_names`` optionally restricts each chunk to the grains backing those
    curated metrics, so a multi-decade backfill of a few daily series pays the
    Mongo/GCS/pod-memory cost of only those grains instead of the whole category.
    ``None``/empty backfills every grain in the category.
    """
    start, end = window
    chunks = backfill_window_chunks(start, end)
    print(
        f"[{_utc_now_iso()}] backfill {category}: {len(chunks)} yearly chunks "
        f"over [{start}, {end})"
    )
    reports = []
    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        title = (
            f"backfill chunk {index}/{len(chunks)} · "
            f"[{chunk_start.date()}, {chunk_end.date()})"
        )
        with _log_group(title):
            reports.append(
                ingest(
                    category=category,
                    window=(chunk_start, chunk_end),
                    raw_format="parquet",
                    context=context,
                    metric_names=metric_names,
                )
            )
    return {
        "category": category,
        "window": [start.to_iso8601_string(), end.to_iso8601_string()],
        "metric_names": list(metric_names) if metric_names else None,
        "chunks": len(chunks),
        "reports": reports,
    }
