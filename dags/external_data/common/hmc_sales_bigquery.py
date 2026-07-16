"""BigQuery load for Hyundai IR "Sales Results": parquet (Cloud Run) -> dl_external.hmc_sales.

The ``dfml-scraper`` Cloud Run Job (HMC file mode) writes one flat parquet covering the
requested year range to GCS; this module loads it into a dedicated typed table. Unlike the
e-Stat Final Report (which rides the ``dl_materials`` commodity star schema), the Hyundai
data is vehicle unit sales across thousands of model/region/plant series, so it lands as a
plain long-format table in ``dl_external`` (the USDA PS&D pattern), not as named metrics.

The load is **idempotent for any year range**: the target table is ensured, the covered
years are deleted, then the parquet is appended. A re-run of the same range replaces exactly
those years and never touches the rest of the table.
"""

from __future__ import annotations

from google.cloud import bigquery

from external_data.common.bigquery_io import ensure_dataset


BQ_TABLE_ID = "hmc_sales"

# Long-format schema, matching the scraper's parquet columns (engine.hmc_sales /
# pipelines.hmc_sales). ``retrieved_at`` is a real UTC timestamp[us] in the parquet, so it
# loads into a native BigQuery TIMESTAMP.
BQ_SCHEMA = [
    bigquery.SchemaField("period", "STRING", description="YYYY-MM reporting month"),
    bigquery.SchemaField("year", "INT64"),
    bigquery.SchemaField("month", "INT64"),
    bigquery.SchemaField("dataset", "STRING", description="sales_by_model / global_plant_sales / export_by_region / us_retail_sales / eu_retail_sales"),
    bigquery.SchemaField("group", "STRING", description="breadcrumb of the row hierarchy, e.g. 'HMI / Domestic'"),
    bigquery.SchemaField("item", "STRING", description="leaf line item: model / country / plant model"),
    bigquery.SchemaField("value", "FLOAT64", description="units sold for the month"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("retrieved_at", "TIMESTAMP", description="UTC fetch timestamp"),
]


def table_fqn(project_id: str, dl_dataset: str) -> str:
    return f"{project_id}.{dl_dataset}.{BQ_TABLE_ID}"


def delete_years_sql(project_id: str, dl_dataset: str) -> str:
    """Parameterized DELETE that clears the covered year range before the append."""

    return (
        f"DELETE FROM `{table_fqn(project_id, dl_dataset)}` "
        "WHERE year BETWEEN @start_year AND @end_year"
    )


def load_parquet_to_bq(
    *,
    parquet_uri: str,
    gcp_project: str,
    location: str,
    dl_dataset: str,
    start_year: int,
    end_year: int,
    client: bigquery.Client | None = None,
) -> int:
    """Load the run's parquet into ``dl_external.hmc_sales`` (delete-range then append).

    Returns the number of rows the parquet load wrote. Idempotent: ensures the dataset and
    table, deletes ``[start_year, end_year]``, then appends — so retries and overlapping
    backfills converge deterministically.
    """

    if not parquet_uri.startswith("gs://"):
        raise ValueError(f"parquet_uri must be a gs:// URI, got {parquet_uri!r}")
    if start_year > end_year:
        raise ValueError("start_year must be <= end_year")

    client = client or bigquery.Client(project=gcp_project, location=location)
    ensure_dataset(client, gcp_project, dl_dataset, location)

    fqn = table_fqn(gcp_project, dl_dataset)
    # Ensure the table exists so the range DELETE never fails on a first run.
    client.create_table(bigquery.Table(fqn, schema=BQ_SCHEMA), exists_ok=True)

    client.query(
        delete_years_sql(gcp_project, dl_dataset),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_year", "INT64", start_year),
                bigquery.ScalarQueryParameter("end_year", "INT64", end_year),
            ]
        ),
    ).result()

    load = client.load_table_from_uri(
        parquet_uri,
        fqn,
        job_config=bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    load.result()
    if load.errors:
        raise RuntimeError(f"BigQuery load failed: {load.errors}")
    return int(load.output_rows or 0)
