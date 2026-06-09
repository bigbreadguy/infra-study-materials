from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.bloomberg_index import to_ndjson, transform_raw_market_data


class BloombergMarketDataTransformTest(TestCase):
    def test_transform_raw_market_data_normalizes_mongo_extended_json(self):
        raw_document = {
            "_id": {"$oid": "69d74d8dd40e21d3cc1bbcd9"},
            "datasetId": {"$oid": "69c38c4df2f7689012b80a74"},
            "ts": {"$date": "1996-04-16T00:00:00Z"},
            "grainId": "SX5E_Index",
            "_schema": "2.1",
            "createdAt": {"$date": "2026-04-09T06:56:13.363Z"},
            "data": {
                "grainId": "SX5E_Index",
                "dt": "1996-04-16",
                "open": 1651.56,
                "high": 1651.56,
                "low": 1651.56,
                "close": 1651.56,
                "volume": 0,
                "oi": 0,
            },
            "updatedAt": {"$date": "2026-04-09T06:56:13.363Z"},
        }
        raw_payload = f"{json.dumps(raw_document)}\n"

        records = transform_raw_market_data(
            raw_payload,
            ingested_at=datetime(2026, 4, 9, 7, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(
            records["grains"],
            [
                {
                    "id": "69c38c4df2f7689012b80a74",
                    "name": "SX5E_Index",
                    "description": "유로 스톡스 50 지수, SX5E",
                }
            ],
        )
        self.assertEqual(len(records["metrics"]), 6)
        self.assertEqual(len(records["metric_values"]), 6)

        open_metric = next(
            metric
            for metric in records["metrics"]
            if metric["name"] == "SX5E_Index_Open"
        )
        open_value = next(
            value
            for value in records["metric_values"]
            if value["id"] == open_metric["id"]
        )

        self.assertEqual(open_metric["grain_id"], "69c38c4df2f7689012b80a74")
        self.assertEqual(open_metric["description"], "유로스톡스 50 시가")
        self.assertEqual(
            open_value,
            {
                "id": open_metric["id"],
                "samle_id": "69d74d8dd40e21d3cc1bbcd9",
                "grain_id": "69c38c4df2f7689012b80a74",
                "grain_name": "SX5E_Index",
                "logical_date": "1996-04-16",
                "time_grain": "D",
                "metric_value": "1651.56",
                "updated_at": "2026-04-09T06:56:13.363Z",
                "ingested_at": "2026-04-09T07:00:00.000000Z",
            },
        )

    def test_to_ndjson_keeps_utf8_descriptions(self):
        payload = to_ndjson([{"description": "유로스톡스 50 시가"}])

        self.assertEqual(payload, '{"description": "유로스톡스 50 시가"}\n')
