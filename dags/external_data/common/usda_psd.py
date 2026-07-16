"""USDA FAS PS&D oilseeds ZIP download, GCS archive, filter, BigQuery load."""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen

from google.cloud import bigquery, storage

PSD_OILSEEDS_ZIP_URL = (
    "https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip"
)
PSD_OILSEEDS_ZIP_NAME = "psd_oilseeds_csv.zip"
PSD_OILSEEDS_CSV_NAME = "psd_oilseeds.csv"
CURATED_CSV_NAME = "psd_us_soybean_production.csv"

# 미국산 대두 생산량 (천 MT). (Local)은 ZIP에 있으나 US+Production 행 없음 → 제외.
FILTER_COMMODITY_DESCRIPTION = "Oilseed, Soybean"
FILTER_COUNTRY_CODE = "US"
FILTER_ATTRIBUTE_DESCRIPTION = "Production"
FILTER_UNIT_ID = "08"

BQ_TABLE_ID = "psd_oilseeds"

BQ_SCHEMA = [
    bigquery.SchemaField("commodity_code", "STRING"),
    bigquery.SchemaField("commodity_description", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("market_year", "INT64"),
    bigquery.SchemaField("calendar_year", "INT64"),
    bigquery.SchemaField("month", "INT64"),
    bigquery.SchemaField("attribute_id", "STRING"),
    bigquery.SchemaField("attribute_description", "STRING"),
    bigquery.SchemaField("unit_id", "STRING"),
    bigquery.SchemaField("unit_description", "STRING"),
    bigquery.SchemaField("value", "FLOAT64"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

_ROW_FIELDNAMES = [f.name for f in BQ_SCHEMA if f.name != "ingested_at"]


def default_gcs_prefix(ingest_date: str) -> str:
    """Hive-style prefix under dfml-dev-raw (no bucket, no trailing slash)."""
    return (
        f"project=external_data/source=usda_psd/dataset=oilseeds"
        f"/ingest_date={ingest_date}"
    )


def gcs_uri(bucket: str, prefix: str, filename: str) -> str:
    return f"gs://{bucket}/{prefix.rstrip('/')}/{filename}"


def download_psd_oilseeds_zip(
    url: str = PSD_OILSEEDS_ZIP_URL,
    *,
    user_agent: str = "dfml-airflow-usda-psd/1.0",
    timeout_sec: int = 300,
) -> bytes:
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout_sec) as response:
        return response.read()


def upload_bytes_to_gcs(
    bucket_name: str,
    object_name: str,
    data: bytes,
    *,
    content_type: str = "application/zip",
) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket_name}/{object_name}"


def _row_matches_filter(row: dict[str, str]) -> bool:
    return (
        row.get("Commodity_Description") == FILTER_COMMODITY_DESCRIPTION
        and row.get("Country_Code") == FILTER_COUNTRY_CODE
        and row.get("Attribute_Description") == FILTER_ATTRIBUTE_DESCRIPTION
        and row.get("Unit_ID") == FILTER_UNIT_ID
    )


def _normalize_row(row: dict[str, str], *, ingested_at: datetime) -> dict[str, Any]:
    return {
        "commodity_code": row.get("Commodity_Code", ""),
        "commodity_description": row.get("Commodity_Description", ""),
        "country_code": row.get("Country_Code", ""),
        "country_name": row.get("Country_Name", ""),
        "market_year": _parse_int(row.get("Market_Year")),
        "calendar_year": _parse_int(row.get("Calendar_Year")),
        "month": _parse_int(row.get("Month")),
        "attribute_id": row.get("Attribute_ID", ""),
        "attribute_description": row.get("Attribute_Description", ""),
        "unit_id": row.get("Unit_ID", ""),
        "unit_description": row.get("Unit_Description", ""),
        "value": _parse_float(row.get("Value")),
        "ingested_at": ingested_at,
    }


def _parse_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    return int(float(raw))


def _parse_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    return float(raw)


def iter_filtered_rows_from_zip(
    zip_bytes: bytes,
    *,
    ingested_at: datetime | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream-filter psd_oilseeds.csv inside the ZIP."""
    ingested = ingested_at or datetime.now(timezone.utc)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        try:
            csv_name = next(
                n for n in zf.namelist() if n.endswith(PSD_OILSEEDS_CSV_NAME)
            )
        except StopIteration as exc:
            raise ValueError(
                f"{PSD_OILSEEDS_CSV_NAME} not found in archive: {zf.namelist()}"
            ) from exc
        with zf.open(csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            reader = csv.DictReader(text)
            for row in reader:
                if _row_matches_filter(row):
                    yield _normalize_row(row, ingested_at=ingested)


def rows_to_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=_ROW_FIELDNAMES, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row[k] for k in _ROW_FIELDNAMES})
    return out.getvalue().encode("utf-8")


def fetch_and_upload_raw(
    *,
    gcs_bucket: str,
    ingest_date: str,
    gcs_prefix: str | None = None,
    zip_url: str = PSD_OILSEEDS_ZIP_URL,
) -> str:
    """Download ZIP and upload to GCS raw. Returns gs:// URI of the ZIP."""
    prefix = (gcs_prefix or default_gcs_prefix(ingest_date)).strip().strip("/")
    zip_bytes = download_psd_oilseeds_zip(url=zip_url)
    object_name = f"{prefix}/{PSD_OILSEEDS_ZIP_NAME}"
    return upload_bytes_to_gcs(gcs_bucket, object_name, zip_bytes)


def transform_and_load_bq(
    *,
    source_zip_uri: str,
    gcp_project: str,
    location: str,
    dl_dataset: str,
    gcs_bucket: str,
    ingest_date: str,
    gcs_prefix: str | None = None,
) -> int:
    """Read ZIP from GCS, filter rows, upload curated CSV, load BigQuery."""
    from external_data.common.bigquery_io import ensure_dataset

    prefix = (gcs_prefix or default_gcs_prefix(ingest_date)).strip().strip("/")
    if not source_zip_uri.startswith(f"gs://{gcs_bucket}/"):
        raise ValueError(f"Unexpected source_zip_uri bucket: {source_zip_uri}")

    object_name = source_zip_uri.removeprefix(f"gs://{gcs_bucket}/")
    client = storage.Client(project=gcp_project)
    zip_bytes = client.bucket(gcs_bucket).blob(object_name).download_as_bytes()

    ingested_at = datetime.now(timezone.utc)
    rows = list(iter_filtered_rows_from_zip(zip_bytes, ingested_at=ingested_at))
    if not rows:
        raise ValueError("No rows matched US soybean production filter")

    by_commodity: dict[str, int] = {}
    for row in rows:
        row["ingested_at"] = ingested_at.isoformat()
        desc = str(row["commodity_description"])
        by_commodity[desc] = by_commodity.get(desc, 0) + 1
    print(f"[USDA PSD] source ZIP: {source_zip_uri}")
    print(f"[USDA PSD] rows by commodity_description: {by_commodity}")

    curated_uri = upload_bytes_to_gcs(
        gcs_bucket,
        f"{prefix}/{CURATED_CSV_NAME}",
        rows_to_csv_bytes(rows),
        content_type="text/csv",
    )
    print(f"[USDA PSD] curated CSV: {curated_uri} ({len(rows)} rows)")

    bq = bigquery.Client(project=gcp_project, location=location)
    ensure_dataset(bq, gcp_project, dl_dataset, location)
    table_fqn = f"{gcp_project}.{dl_dataset}.{BQ_TABLE_ID}"
    job = bq.load_table_from_json(
        rows,
        table_fqn,
        job_config=bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    if job.errors:
        raise RuntimeError(f"BigQuery load failed: {job.errors}")
    print(f"[USDA PSD] loaded {len(rows)} rows -> {table_fqn}")
    return len(rows)
