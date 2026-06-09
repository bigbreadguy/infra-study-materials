from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


DEFAULT_SAMPLE_ID_FIELD = "sample_id"
TIME_GRAIN = "D"
RAW_CREATED_AT_FIELD = "createdAt"
RAW_DATA_FIELD = "data"
RAW_DATASET_ID_FIELD = "datasetId"
RAW_GRAIN_ID_FIELD = "grainId"
RAW_LOGICAL_DATE_FIELD = "dt"
RAW_SAMPLE_ID_FIELD = "_id"
RAW_SCHEMA_FIELD = "_schema"
RAW_TIMESTAMP_FIELD = "ts"
RAW_UPDATED_AT_FIELD = "updatedAt"

GRAIN_DESCRIPTIONS = {
    "SX5E_Index": "유로 스톡스 50 지수, SX5E",
}

METRIC_FIELDS = {
    "open": ("Open", "시가"),
    "high": ("High", "고가"),
    "low": ("Low", "저가"),
    "close": ("Close", "종가"),
    "volume": ("Volume", "거래량"),
    "oi": ("OpenInterest", "미결제약정"),
}

METRIC_DESCRIPTIONS = {
    ("SX5E_Index", "open"): "유로스톡스 50 시가",
    ("SX5E_Index", "high"): "유로스톡스 50 고가",
    ("SX5E_Index", "low"): "유로스톡스 50 저가",
    ("SX5E_Index", "close"): "유로스톡스 50 종가",
    ("SX5E_Index", "volume"): "유로스톡스 50 거래량",
    ("SX5E_Index", "oi"): "유로스톡스 50 미결제약정",
}


def _isoformat_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _coerce_extended_date(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, int | float):
        return _isoformat_utc(datetime.fromtimestamp(value / 1000, tz=timezone.utc))
    if isinstance(value, dict):
        coerced_value = _coerce_extended_json(value)
        if isinstance(coerced_value, int | float):
            return _isoformat_utc(
                datetime.fromtimestamp(coerced_value / 1000, tz=timezone.utc)
            )

    raise ValueError(f"Unsupported Mongo Extended JSON date value: {value!r}")


def _coerce_extended_json(value: Any) -> Any:
    if isinstance(value, list):
        return [_coerce_extended_json(item) for item in value]
    if not isinstance(value, dict):
        return value

    keys = set(value)
    if keys == {"$oid"}:
        return str(value["$oid"])
    if keys == {"$date"}:
        return _coerce_extended_date(value["$date"])
    if keys == {"$numberInt"}:
        return int(value["$numberInt"])
    if keys == {"$numberLong"}:
        return int(value["$numberLong"])
    if keys == {"$numberDouble"}:
        return float(value["$numberDouble"])
    if keys == {"$numberDecimal"}:
        return str(value["$numberDecimal"])

    return {key: _coerce_extended_json(item) for key, item in value.items()}


def _raw_documents(raw_payload: str) -> list[dict[str, Any]]:
    documents = []

    for line_number, line in enumerate(raw_payload.splitlines(), start=1):
        stripped_line = line.strip()
        if not stripped_line:
            continue

        try:
            raw_document = json.loads(stripped_line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON on raw NDJSON line {line_number}") from exc

        document = _coerce_extended_json(raw_document)
        if not isinstance(document, dict):
            raise ValueError(f"Raw NDJSON line {line_number} must contain a JSON object")

        documents.append(document)

    return documents


def _required_text(source: dict[str, Any], key: str, line_number: int) -> str:
    value = source.get(key)
    if value is None or value == "":
        raise ValueError(f"Raw document line {line_number} requires field {key!r}")

    return str(value)


def _required_mapping(
    source: dict[str, Any], key: str, line_number: int
) -> dict[str, Any]:
    value = source.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Raw document line {line_number} requires object field {key!r}")

    return value


def _logical_date(document: dict[str, Any], data: dict[str, Any], line_number: int) -> str:
    data_date = data.get(RAW_LOGICAL_DATE_FIELD)
    if isinstance(data_date, str) and data_date:
        return data_date

    ts = document.get(RAW_TIMESTAMP_FIELD)
    if isinstance(ts, str) and len(ts) >= 10:
        return ts[:10]

    raise ValueError(
        f"Raw document line {line_number} requires data.dt or ISO timestamp ts"
    )


def _metric_name(grain_name: str, metric_key: str) -> str:
    metric_suffix = METRIC_FIELDS[metric_key][0]
    return f"{grain_name}_{metric_suffix}"


def _metric_description(grain_name: str, metric_key: str) -> str:
    metric_description = METRIC_DESCRIPTIONS.get((grain_name, metric_key))
    if metric_description:
        return metric_description

    return _metric_name(grain_name, metric_key)


def _metric_value(value: Any) -> str:
    return str(value)


def transform_raw_market_data(
    raw_payload: str,
    *,
    ingested_at: datetime | None = None,
    sample_id_field: str = DEFAULT_SAMPLE_ID_FIELD,
) -> dict[str, list[dict[str, Any]]]:
    if not sample_id_field:
        raise ValueError("sample_id_field must be a non-empty string")

    ingested_at_text = _isoformat_utc(ingested_at or datetime.now(timezone.utc))
    grain_records: dict[str, dict[str, Any]] = {}
    metric_records: dict[str, dict[str, Any]] = {}
    metric_value_records = []

    for line_number, document in enumerate(_raw_documents(raw_payload), start=1):
        data = _required_mapping(document, RAW_DATA_FIELD, line_number)
        sample_id = _required_text(document, RAW_SAMPLE_ID_FIELD, line_number)
        dataset_id = _required_text(document, RAW_DATASET_ID_FIELD, line_number)
        grain_name = _required_text(document, RAW_GRAIN_ID_FIELD, line_number)
        updated_at = _required_text(document, RAW_UPDATED_AT_FIELD, line_number)
        logical_date = _logical_date(document, data, line_number)

        grain_records.setdefault(
            dataset_id,
            {
                "dataset_id": dataset_id,
                "name": grain_name,
                "description": GRAIN_DESCRIPTIONS.get(grain_name, grain_name),
            },
        )

        for metric_key in METRIC_FIELDS:
            if metric_key not in data or data[metric_key] is None:
                continue

            metric_name = _metric_name(grain_name, metric_key)
            metric_records.setdefault(
                metric_name,
                {
                    "grain_dataset_id": dataset_id,
                    "name": metric_name,
                    "description": _metric_description(grain_name, metric_key),
                },
            )
            metric_value_records.append(
                {
                    sample_id_field: sample_id,
                    "grain_dataset_id": dataset_id,
                    "grain_name": grain_name,
                    "metric_name": metric_name,
                    "logical_date": logical_date,
                    "time_grain": TIME_GRAIN,
                    "metric_value": _metric_value(data[metric_key]),
                    "updated_at": updated_at,
                    "ingested_at": ingested_at_text,
                }
            )

    return {
        "grains": list(grain_records.values()),
        "metrics": list(metric_records.values()),
        "metric_values": metric_value_records,
    }


def to_ndjson(records: list[dict[str, Any]]) -> str:
    if not records:
        return ""

    return "".join(
        f"{json.dumps(record, ensure_ascii=False)}\n" for record in records
    )
