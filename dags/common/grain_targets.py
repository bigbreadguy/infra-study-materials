from __future__ import annotations

import json
import re


GRAIN_TARGETS_VARIABLE = "mongo_grain_targets"

_OBJECT_ID_PATTERN = re.compile(r"^[0-9a-f]{24}$")

# The freq value is embedded as a SQL string literal in the fact values
# merge, so it stays restricted to a short alphanumeric token.
_FREQ_PATTERN = re.compile(r"^[A-Za-z0-9]{1,8}$")


def parse_grain_targets(raw: str | list) -> list[dict]:
    """Parse and validate the JSON-structured grain targets variable.

    Returns only enabled targets, each as a dict with dataset_id, grain_id,
    and description keys. Raises ValueError with a precise message on any
    malformed entry so a bad variable edit fails fast in one obvious place.
    """
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} must be valid json: {exc}"
            ) from exc
    else:
        parsed = raw

    if not isinstance(parsed, list) or not parsed:
        raise ValueError(
            f"{GRAIN_TARGETS_VARIABLE} must be a non-empty json array"
        )

    targets = []
    seen_grain_ids = set()
    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE}[{index}] must be a json object"
            )

        grain_id = entry.get("grain_id")
        if not isinstance(grain_id, str) or not grain_id:
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE}[{index}] must set grain_id "
                "to a non-empty string"
            )
        if grain_id in seen_grain_ids:
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} has duplicate grain_id {grain_id}"
            )
        seen_grain_ids.add(grain_id)

        dataset_id = entry.get("dataset_id")
        if not isinstance(dataset_id, str) or not _OBJECT_ID_PATTERN.fullmatch(
            dataset_id
        ):
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} entry {grain_id} must set "
                "dataset_id to a twenty four character lowercase hex object id"
            )

        description = entry.get("description")
        if not isinstance(description, str) or not description:
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} entry {grain_id} must set "
                "description to a non-empty string"
            )
        # The description is embedded as a SQL string literal in the
        # dim grains merge; reject the characters the literal builder
        # refuses so a bad variable edit fails here, not mid pipeline.
        if "'" in description or "\\" in description:
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} entry {grain_id} description "
                "must not contain quotes or backslashes"
            )

        freq = entry.get("freq")
        if not isinstance(freq, str) or not _FREQ_PATTERN.fullmatch(freq):
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} entry {grain_id} must set freq "
                "to a short alphanumeric time grain string"
            )

        enabled = entry.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(
                f"{GRAIN_TARGETS_VARIABLE} entry {grain_id} must set "
                "enabled to a boolean when present"
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
        raise ValueError(
            f"{GRAIN_TARGETS_VARIABLE} must enable at least one grain"
        )

    return targets
