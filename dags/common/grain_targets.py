from __future__ import annotations

import json
import re
from pathlib import Path


# Grain target config files are local-only (gitignored via /dags/local/) but
# live under dags/ so the compose volume mount makes them visible in every
# Airflow container.
CONFIG_DIR = Path(__file__).resolve().parent.parent / "local" / "grain_targets"

_OBJECT_ID_PATTERN = re.compile(r"^[0-9a-f]{24}$")

# The freq value is embedded as a SQL string literal in the fact values
# merge, so it stays restricted to a short alphanumeric token.
_FREQ_PATTERN = re.compile(r"^[A-Za-z0-9]{1,8}$")


def parse_grain_targets(raw: str | list, source: str) -> list[dict]:
    """Parse and validate JSON-structured grain targets.

    Returns only enabled targets, each as a dict with dataset_id, grain_id,
    description, and freq keys. Raises ValueError with a precise message on
    any malformed entry so a bad config edit fails fast in one obvious place.
    """
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{source} must be valid json: {exc}") from exc
    else:
        parsed = raw

    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"{source} must be a non-empty json array")

    targets = []
    seen_grain_keys = set()
    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            raise ValueError(f"{source}[{index}] must be a json object")

        grain_id = entry.get("grain_id")
        if not isinstance(grain_id, str) or not grain_id:
            raise ValueError(
                f"{source}[{index}] must set grain_id to a non-empty string"
            )

        dataset_id = entry.get("dataset_id")
        if not isinstance(dataset_id, str) or not _OBJECT_ID_PATTERN.fullmatch(
            dataset_id
        ):
            raise ValueError(
                f"{source} entry {grain_id} must set "
                "dataset_id to a twenty four character lowercase hex object id"
            )

        # Grain identity is the dataset id and grain id pair, matching the
        # Mongo compound key and the BigQuery dim merge keys: the same
        # grain_id legitimately recurs under several datasets (e.g. one
        # copper_total_prod per mining company).
        grain_key = (dataset_id, grain_id)
        if grain_key in seen_grain_keys:
            raise ValueError(
                f"{source} has duplicate grain_id {grain_id} "
                f"for dataset_id {dataset_id}"
            )
        seen_grain_keys.add(grain_key)

        description = entry.get("description")
        if not isinstance(description, str) or not description:
            raise ValueError(
                f"{source} entry {grain_id} must set "
                "description to a non-empty string"
            )
        # The description is embedded as a SQL string literal in the
        # dim grains merge; reject the characters the literal builder refuses
        # so a bad config edit fails here, not mid pipeline.
        if "'" in description or "\\" in description:
            raise ValueError(
                f"{source} entry {grain_id} description "
                "must not contain quotes or backslashes"
            )

        freq = entry.get("freq")
        if not isinstance(freq, str) or not _FREQ_PATTERN.fullmatch(freq):
            raise ValueError(
                f"{source} entry {grain_id} must set freq "
                "to a short alphanumeric time grain string"
            )

        enabled = entry.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(
                f"{source} entry {grain_id} must set enabled to a boolean "
                "when present"
            )
        if not enabled:
            continue

        targets.append(
            {
                "dataset_id": dataset_id,
                "grain_id": grain_id,
                "description": description,
                "freq": freq,
            }
        )

    if not targets:
        raise ValueError(f"{source} must enable at least one grain")

    return targets


def list_grain_target_categories() -> list[str]:
    """Sorted category names derived from the json file stems."""
    return sorted(path.stem for path in CONFIG_DIR.glob("*.json") if path.is_file())


def load_grain_targets(category: str) -> list[dict]:
    """Read and parse the grain targets for one configured category."""
    config_file = CONFIG_DIR / f"{category}.json"
    return parse_grain_targets(
        config_file.read_text(encoding="utf-8"),
        source=f"grain targets file {category}.json",
    )
