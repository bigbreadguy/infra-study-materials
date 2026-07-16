"""BigQuery helpers for external-data pipelines."""

from __future__ import annotations

from google.cloud import bigquery


def ensure_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
    *,
    labels: dict[str, str] | None = None,
) -> None:
    """Create dataset if missing (idempotent)."""
    ref = bigquery.DatasetReference(project_id, dataset_id)
    ds = bigquery.Dataset(ref)
    ds.location = location
    if labels:
        ds.labels = labels
    client.create_dataset(ds, exists_ok=True)
