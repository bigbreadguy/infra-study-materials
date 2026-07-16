"""Selection logic for dpanda grain catalog documents.

The catalog collection holds one document per catalog variable with grain
identity nested at process.grainId / process.datasetId. Several documents can
share one grainId: metric-suffix variants (X, X.open, X.close, ...) carry an
identical info object, while a few grain ids are reused by genuinely different
variables whose process.datasetId disambiguates against the grain target
dataset_id. The catalog datasetId can also lag or diverge from the grain
target dataset_id, so the lookup must start from grainId alone.
"""

from __future__ import annotations

import json

from external_data.common.dpanda_bloomberg_ingest import (
    CATEGORIES,
    materials_recipe,
)
from external_data.common.materials_metrics import grain_targets_from_config


def collect_unique_grain_targets() -> list[dict]:
    """Union of every category's grain targets, deduplicated.

    Each category's grains come from its materials config
    (``configs/materials_metrics/dpanda_bloomberg.<category>.json``), the single
    source of truth for grain identity, description, and freq. Identity follows
    dim_grains: the (dataset_id, grain_id) pair. The same pair named in two
    category configs yields one target; the same grain_id under two dataset_ids
    stays two targets.
    """
    targets = []
    seen_keys = set()
    for category in CATEGORIES:
        for target in grain_targets_from_config(materials_recipe(category)):
            key = (target["dataset_id"], target["grain_id"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            targets.append(target)

    return targets


def _info_key(document: dict) -> str:
    return json.dumps(document.get("info"), sort_keys=True, default=str)


def select_catalog_document(
    documents: list[dict],
    *,
    dataset_id: str,
    grain_id: str,
) -> dict | None:
    """Pick the one catalog document whose info describes this grain target.

    documents are the catalog matches for process.grainId == grain_id. When
    any of them carries the grain target's dataset_id at process.datasetId,
    only those count; otherwise every match stays a candidate because the
    catalog datasetId legitimately diverges from the target dataset_id for
    some grains. Candidates must then agree on one info object (metric-suffix
    variants do); disagreement means the catalog and the grain target config
    no longer line up, which must fail loudly for human review.

    Returns None when no document matches at all. Among agreeing candidates
    the lexicographically smallest _id wins, which is the base variable code
    next to its metric-suffix variants.
    """
    matching = [
        document
        for document in documents
        if str(document.get("process", {}).get("datasetId")) == dataset_id
    ]
    candidates = matching or documents
    if not candidates:
        return None

    distinct_infos = {_info_key(document) for document in candidates}
    if len(distinct_infos) > 1:
        catalog_ids = sorted(str(document.get("_id")) for document in candidates)
        raise ValueError(
            f"grainId {grain_id} with dataset_id {dataset_id} resolves "
            f"{len(distinct_infos)} distinct catalog info objects across "
            f"catalog documents {catalog_ids}; align the grain targets "
            "config with the catalog collection"
        )

    return min(candidates, key=lambda document: str(document.get("_id")))
