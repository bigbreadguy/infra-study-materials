from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.dpanda_index import (
    METRIC_FIELDS,
    RAW_CREATED_AT_FIELD,
    RAW_DATA_FIELD,
    RAW_DATASET_ID_FIELD,
    RAW_GRAIN_ID_FIELD,
    RAW_LOGICAL_DATE_FIELD,
    RAW_SAMPLE_ID_FIELD,
    RAW_SCHEMA_FIELD,
    RAW_TIMESTAMP_FIELD,
    RAW_UPDATED_AT_FIELD,
    to_ndjson,
    transform_raw_market_data,
)


SYNTHETIC_DATASET_ID = "dataset-example-001"
SYNTHETIC_GRAIN_NAME = "ANON_Index"
SYNTHETIC_LOGICAL_DATE = "2000-01-02"
SYNTHETIC_SAMPLE_ID = "sample-example-001"
SYNTHETIC_UPDATED_AT = "2000-01-03T04:05:06.789Z"
SYNTHETIC_VALUE = 123.45
SYNTHETIC_ZERO_VALUE = 0


def _synthetic_raw_document():
    raw_values = {}
    value_fields = list(METRIC_FIELDS)
    for metric_field in value_fields[:4]:
        raw_values[metric_field] = SYNTHETIC_VALUE
    for metric_field in value_fields[4:]:
        raw_values[metric_field] = SYNTHETIC_ZERO_VALUE

    return {
        RAW_SAMPLE_ID_FIELD: {"$oid": SYNTHETIC_SAMPLE_ID},
        RAW_DATASET_ID_FIELD: {"$oid": SYNTHETIC_DATASET_ID},
        RAW_TIMESTAMP_FIELD: {"$date": f"{SYNTHETIC_LOGICAL_DATE}T00:00:00Z"},
        RAW_GRAIN_ID_FIELD: SYNTHETIC_GRAIN_NAME,
        RAW_SCHEMA_FIELD: "test-schema-version",
        RAW_CREATED_AT_FIELD: {"$date": "2000-01-01T00:00:00.000Z"},
        RAW_DATA_FIELD: {
            RAW_GRAIN_ID_FIELD: SYNTHETIC_GRAIN_NAME,
            RAW_LOGICAL_DATE_FIELD: SYNTHETIC_LOGICAL_DATE,
            **raw_values,
        },
        RAW_UPDATED_AT_FIELD: {"$date": SYNTHETIC_UPDATED_AT},
    }


class DpandaMarketDataTransformTest(TestCase):
    def test_transform_raw_market_data_normalizes_mongo_extended_json(self):
        raw_payload = f"{json.dumps(_synthetic_raw_document())}\n"

        records = transform_raw_market_data(
            raw_payload,
            ingested_at=datetime(2000, 1, 3, 7, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(
            records["grains"],
            [
                {
                    "dataset_id": SYNTHETIC_DATASET_ID,
                    "name": SYNTHETIC_GRAIN_NAME,
                    "description": SYNTHETIC_GRAIN_NAME,
                }
            ],
        )
        self.assertEqual(len(records["metrics"]), 6)
        self.assertEqual(len(records["metric_values"]), 6)

        primary_metric_field = next(iter(METRIC_FIELDS))
        primary_metric_name = (
            f"{SYNTHETIC_GRAIN_NAME}_{METRIC_FIELDS[primary_metric_field][0]}"
        )
        primary_metric = next(
            metric
            for metric in records["metrics"]
            if metric["name"] == primary_metric_name
        )
        primary_value = next(
            value
            for value in records["metric_values"]
            if value["metric_name"] == primary_metric["name"]
        )

        self.assertEqual(primary_metric["grain_dataset_id"], SYNTHETIC_DATASET_ID)
        self.assertEqual(primary_metric["description"], primary_metric["name"])
        self.assertEqual(
            primary_value,
            {
                "sample_id": SYNTHETIC_SAMPLE_ID,
                "grain_dataset_id": SYNTHETIC_DATASET_ID,
                "grain_name": SYNTHETIC_GRAIN_NAME,
                "metric_name": primary_metric["name"],
                "logical_date": SYNTHETIC_LOGICAL_DATE,
                "time_grain": "D",
                "metric_value": str(SYNTHETIC_VALUE),
                "updated_at": SYNTHETIC_UPDATED_AT,
                "ingested_at": "2000-01-03T07:00:00.000000Z",
            },
        )

    def test_to_ndjson_keeps_utf8_descriptions(self):
        payload = to_ndjson([{"description": "테스트 설명"}])

        self.assertEqual(payload, '{"description": "테스트 설명"}\n')
