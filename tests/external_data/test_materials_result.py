from __future__ import annotations

import json
from unittest import TestCase

from external_data.common.materials_result import (
    envelope_records,
    envelope_to_ndjson,
    estat_time_to_ym,
    iso_timestamp_to_date,
    normalize_estat_periods,
    normalize_iso_timestamp_periods,
    normalize_quarter_periods,
    parse_period_ym,
    quarter_period_to_iso,
    records_to_ndjson,
    rename_estat_keys,
    derive_lme_cot_summary,
    slice_records_to_requested_range,
    stamp_contract_ranks,
)


def _success_envelope() -> dict:
    return {
        "status": "success",
        "recipe": "kosa.steel_scrap_import",
        "data": [
            {"시점": "2024.01", "국내수입 물량": "115"},
            {"시점": "2024.02", "국내수입 물량": "184"},
        ],
    }


class EnvelopeRecordsTests(TestCase):
    def test_success_returns_records(self):
        self.assertEqual(len(envelope_records(_success_envelope())), 2)

    def test_failed_returns_empty(self):
        env = _success_envelope()
        env["status"] = "failed"
        self.assertEqual(envelope_records(env), [])

    def test_null_data_returns_empty(self):
        env = {"status": "success", "data": None}
        self.assertEqual(envelope_records(env), [])

    def test_non_dict_record_raises(self):
        env = {"status": "success", "data": ["not-an-object"]}
        with self.assertRaises(ValueError):
            envelope_records(env)


class RecordsToNdjsonTests(TestCase):
    def test_wraps_each_record_under_row(self):
        ndjson = records_to_ndjson([{"시점": "2024.01", "국내수입 물량": "115"}])
        lines = ndjson.splitlines()
        self.assertEqual(len(lines), 1)
        parsed = json.loads(lines[0])
        self.assertEqual(list(parsed.keys()), ["row"])
        self.assertEqual(parsed["row"]["국내수입 물량"], "115")

    def test_preserves_korean_unescaped(self):
        ndjson = records_to_ndjson([{"품목명": "용해용철스크랩"}])
        self.assertIn("용해용철스크랩", ndjson)

    def test_empty_records_yield_empty_string(self):
        self.assertEqual(records_to_ndjson([]), "")

    def test_nan_value_coerced_to_json_null(self):
        # yfinance yields NaN for a not-yet-settled day; the bare ``NaN`` token is
        # invalid JSON and breaks BigQuery's NDJSON reader. It must become null.
        ndjson = records_to_ndjson(
            [{"date": "2026-06-24", "try_usd_exchange_rate": float("nan")}]
        )
        self.assertNotIn("NaN", ndjson)
        parsed = json.loads(ndjson)
        self.assertIsNone(parsed["row"]["try_usd_exchange_rate"])
        self.assertEqual(parsed["row"]["date"], "2026-06-24")

    def test_infinity_values_coerced_to_json_null(self):
        ndjson = records_to_ndjson(
            [{"hi": float("inf"), "lo": float("-inf"), "ok": 1.5}]
        )
        self.assertNotIn("Infinity", ndjson)
        parsed = json.loads(ndjson)
        self.assertIsNone(parsed["row"]["hi"])
        self.assertIsNone(parsed["row"]["lo"])
        self.assertEqual(parsed["row"]["ok"], 1.5)

    def test_finite_floats_and_strings_preserved(self):
        # The sanitizer must not touch finite numbers or string values.
        ndjson = records_to_ndjson(
            [{"date": "2026-06-23", "try_usd_exchange_rate": 0.02152084745466709}]
        )
        parsed = json.loads(ndjson)
        self.assertEqual(parsed["row"]["try_usd_exchange_rate"], 0.02152084745466709)

    def test_one_line_per_record(self):
        ndjson = envelope_to_ndjson(_success_envelope())
        self.assertEqual(len(ndjson.splitlines()), 2)


def _ytd_records(*months: str) -> list[dict]:
    # One row per "YYYY.MM" month, mirroring a kosa result's data[] shape.
    return [{"시점": m, "국내수입 물량": "100"} for m in months]


def _params(start: str, end: str) -> dict:
    sy, sm = start.split(".")
    ey, em = end.split(".")
    return {"start_year": sy, "start_month": sm, "end_year": ey, "end_month": em}


class ParsePeriodYmTests(TestCase):
    def test_parses_dotted_year_month(self):
        self.assertEqual(parse_period_ym("2024.01"), (2024, 1))

    def test_rejects_missing_period(self):
        with self.assertRaises(ValueError):
            parse_period_ym(None)

    def test_rejects_malformed_period(self):
        with self.assertRaises(ValueError):
            parse_period_ym("2024-01")


class SliceRecordsToRequestedRangeTests(TestCase):
    def test_past_year_exact_range_is_no_op(self):
        # KOSA returns the exact range for a past year; slice keeps every row.
        records = _ytd_records("2024.01", "2024.02", "2024.03")
        sliced = slice_records_to_requested_range(records, _params("2024.01", "2024.03"))
        self.assertEqual([r["시점"] for r in sliced], ["2024.01", "2024.02", "2024.03"])

    def test_current_year_published_month_slices_to_requested(self):
        # Asked 2026.01 but got the whole published YTD; keep only the requested month.
        ytd = _ytd_records("2026.01", "2026.02", "2026.03", "2026.04", "2026.05")
        sliced = slice_records_to_requested_range(ytd, _params("2026.01", "2026.01"))
        self.assertEqual([r["시점"] for r in sliced], ["2026.01"])

    def test_current_year_multi_month_range(self):
        ytd = _ytd_records("2026.01", "2026.02", "2026.03", "2026.04", "2026.05")
        sliced = slice_records_to_requested_range(ytd, _params("2026.02", "2026.03"))
        self.assertEqual([r["시점"] for r in sliced], ["2026.02", "2026.03"])

    def test_unpublished_month_yields_empty_slice(self):
        # Requested month not yet in the published YTD chunk -> benign empty slice.
        ytd = _ytd_records("2026.01", "2026.02", "2026.03")
        self.assertEqual(slice_records_to_requested_range(ytd, _params("2026.06", "2026.06")), [])

    def test_slice_is_idempotent(self):
        # Re-running on already-sliced rows is a no-op.
        ytd = _ytd_records("2026.01", "2026.02", "2026.03", "2026.04", "2026.05")
        once = slice_records_to_requested_range(ytd, _params("2026.01", "2026.02"))
        twice = slice_records_to_requested_range(once, _params("2026.01", "2026.02"))
        self.assertEqual(once, twice)

    def test_empty_records_returns_empty(self):
        self.assertEqual(slice_records_to_requested_range([], _params("2024.01", "2024.03")), [])

    def test_missing_params_raises(self):
        with self.assertRaises(ValueError):
            slice_records_to_requested_range(_ytd_records("2024.01"), None)

    def test_malformed_period_in_row_raises(self):
        with self.assertRaises(ValueError):
            slice_records_to_requested_range(
                [{"시점": "garbage"}], _params("2024.01", "2024.03")
            )


class QuarterPeriodTests(TestCase):
    def test_each_quarter_maps_to_its_start_month(self):
        self.assertEqual(quarter_period_to_iso("2026-Q1"), "2026-01-01")
        self.assertEqual(quarter_period_to_iso("2026-Q2"), "2026-04-01")
        self.assertEqual(quarter_period_to_iso("2026-Q3"), "2026-07-01")
        self.assertEqual(quarter_period_to_iso("2025-Q4"), "2025-10-01")

    def test_non_quarter_value_passes_through_unchanged(self):
        # Idempotent: an already-converted date (or any non-quarter string) is returned
        # as-is, so applying the conversion twice is safe.
        self.assertEqual(quarter_period_to_iso("2026-01-01"), "2026-01-01")
        self.assertEqual(quarter_period_to_iso("2026-13"), "2026-13")
        self.assertEqual(quarter_period_to_iso(None), None)

    def test_normalize_rewrites_period_column_without_mutating_input(self):
        records = [
            {"period": "2026-Q1", "seriesId": "COPR_VE", "value": 0.87},
            {"period": "2025-Q4", "seriesId": "COPR_VE", "value": 0.91},
        ]
        out = normalize_quarter_periods(records, "period")
        self.assertEqual([r["period"] for r in out], ["2026-01-01", "2025-10-01"])
        # Other fields are preserved; the originals are not mutated.
        self.assertEqual(out[0]["seriesId"], "COPR_VE")
        self.assertEqual(records[0]["period"], "2026-Q1")

    def test_normalize_skips_records_without_the_period_column(self):
        records = [{"value": 1.0}]
        self.assertEqual(normalize_quarter_periods(records, "period"), [{"value": 1.0}])


class EstatPeriodTests(TestCase):
    def test_monthly_code_maps_to_year_month(self):
        # e-Stat YYYY00MMMM monthly codes: 2024000303 -> 2024-03, 2020001111 -> 2020-11.
        self.assertEqual(estat_time_to_ym("2024000303"), "2024-03")
        self.assertEqual(estat_time_to_ym("2020001111"), "2020-11")
        self.assertEqual(estat_time_to_ym("2015000101"), "2015-01")
        self.assertEqual(estat_time_to_ym("2026001212"), "2026-12")

    def test_non_estat_value_passes_through_unchanged(self):
        # Idempotent: an already-normalized YYYY-MM, an out-of-range month, or non-str
        # is returned as-is, so applying the conversion twice is safe.
        self.assertEqual(estat_time_to_ym("2024-03"), "2024-03")
        self.assertEqual(estat_time_to_ym("2024001313"), "2024001313")  # month 13 invalid
        self.assertEqual(estat_time_to_ym(None), None)

    def test_normalize_rewrites_time_column_without_mutating_input(self):
        records = [
            {"@time": "2024000303", "@cat01": "110", "$": "4131462"},
            {"@time": "2024000202", "@cat01": "110", "$": "4049862"},
        ]
        out = normalize_estat_periods(records, "@time")
        self.assertEqual([r["@time"] for r in out], ["2024-03", "2024-02"])
        self.assertEqual(out[0]["@cat01"], "110")
        self.assertEqual(records[0]["@time"], "2024000303")

    def test_normalize_skips_records_without_the_period_column(self):
        records = [{"$": "1"}]
        self.assertEqual(normalize_estat_periods(records, "@time"), [{"$": "1"}])


class EstatKeyRenameTests(TestCase):
    def test_renames_at_and_dollar_keys_to_plain_names(self):
        records = [
            {"@tab": "550", "@cat01": "110", "@time": "2024000303", "@unit": "kl", "$": "4131462"},
            {"@tab": "160", "@area": "50000", "@time": "2024000303", "@unit": "kl", "$": "11808409"},
        ]
        out = rename_estat_keys(records)
        self.assertEqual(
            out[0],
            {"tab": "550", "cat01": "110", "time": "2024000303", "unit": "kl", "value": "4131462"},
        )
        self.assertEqual(out[1]["area"], "50000")
        self.assertEqual(out[1]["value"], "11808409")
        # Originals untouched.
        self.assertEqual(records[0]["$"], "4131462")

    def test_unmapped_keys_pass_through(self):
        self.assertEqual(rename_estat_keys([{"other": 1}]), [{"other": 1}])

    def test_rename_then_normalize_yields_clean_staging_rows(self):
        # Mirrors the pipeline's estat staging: rename keys, then rewrite the time code.
        records = [{"@cat01": "110", "@time": "2024000303", "$": "4131462"}]
        out = normalize_estat_periods(rename_estat_keys(records), "time")
        self.assertEqual(out, [{"cat01": "110", "time": "2024-03", "value": "4131462"}])


class IsoTimestampPeriodTests(TestCase):
    def test_timestamp_strips_to_leading_date(self):
        # CFTC report dates arrive as Socrata floating timestamps.
        self.assertEqual(
            iso_timestamp_to_date("2026-06-23T00:00:00.000"), "2026-06-23"
        )
        self.assertEqual(
            iso_timestamp_to_date("1986-01-07T00:00:00.000"), "1986-01-07"
        )

    def test_non_timestamp_value_passes_through_unchanged(self):
        # Idempotent: an already-normalized date, a non-matching string, or a non-str
        # is returned as-is, so applying the conversion twice is safe.
        self.assertEqual(iso_timestamp_to_date("2026-06-23"), "2026-06-23")
        self.assertEqual(iso_timestamp_to_date("not-a-date"), "not-a-date")
        self.assertEqual(iso_timestamp_to_date(None), None)

    def test_normalize_rewrites_period_column_without_mutating_input(self):
        records = [
            {"report_date_as_yyyy_mm_dd": "2026-06-23T00:00:00.000", "commodity": "COPPER"},
            {"report_date_as_yyyy_mm_dd": "2026-06-16T00:00:00.000", "commodity": "COPPER"},
        ]
        out = normalize_iso_timestamp_periods(records, "report_date_as_yyyy_mm_dd")
        self.assertEqual(
            [r["report_date_as_yyyy_mm_dd"] for r in out],
            ["2026-06-23", "2026-06-16"],
        )
        self.assertEqual(out[0]["commodity"], "COPPER")
        # Originals untouched.
        self.assertEqual(
            records[0]["report_date_as_yyyy_mm_dd"], "2026-06-23T00:00:00.000"
        )

    def test_normalize_skips_records_without_the_period_column(self):
        records = [{"commodity": "COPPER"}]
        self.assertEqual(
            normalize_iso_timestamp_periods(records, "report_date_as_yyyy_mm_dd"),
            [{"commodity": "COPPER"}],
        )


class StampContractRanksTests(TestCase):
    def test_rank_is_the_month_delta_from_the_trading_day(self):
        # The rank is the calendar month delta between the trading day's month and
        # the delivery month -- a per-row fact, independent of what else is listed.
        records = [
            {"date": "2024-07-10", "delivery_month": "2024-09", "settle": 20},
            {"date": "2024-07-10", "delivery_month": "2024-07", "settle": 10},
            {"date": "2024-07-10", "delivery_month": "2024-08", "settle": 15},
            {"date": "2024-07-11", "delivery_month": "2025-07", "settle": 16},
            {"date": "2024-07-11", "delivery_month": "2024-07", "settle": 11},
        ]
        out = stamp_contract_ranks(records, period_column="date")
        self.assertEqual(
            [(r["date"], r["delivery_month"], r["contract_rank"]) for r in out],
            [
                ("2024-07-10", "2024-09", "2"),
                ("2024-07-10", "2024-07", "0"),
                ("2024-07-10", "2024-08", "1"),
                ("2024-07-11", "2025-07", "12"),
                ("2024-07-11", "2024-07", "0"),
            ],
        )

    def test_rank_is_a_string_and_zero_is_the_spot_month(self):
        records = [
            {"date": "2024-07-10", "delivery_month": "2024-12", "settle": 1},
            {"date": "2024-07-10", "delivery_month": "2024-07", "settle": 2},
        ]
        out = stamp_contract_ranks(records, period_column="date")
        front = next(r for r in out if r["delivery_month"] == "2024-07")
        self.assertEqual(front["contract_rank"], "0")
        self.assertIsInstance(front["contract_rank"], str)

    def test_no_rank_zero_once_the_spot_contract_is_delisted(self):
        # Late in the month the spot contract is gone; the nearest listed contract
        # keeps its true delta ("1"), it is NOT promoted to "0".
        records = [
            {"date": "2024-07-22", "delivery_month": "2024-08", "settle": 1},
            {"date": "2024-07-22", "delivery_month": "2024-09", "settle": 2},
        ]
        out = stamp_contract_ranks(records, period_column="date")
        self.assertEqual([r["contract_rank"] for r in out], ["1", "2"])

    def test_never_drops_rows_and_leaves_originals_untouched(self):
        records = [
            {"date": "2024-07-10", "delivery_month": "2025-06", "settle": 1},
            {"date": "2024-07-10", "delivery_month": "2024-07", "settle": 2},
        ]
        out = stamp_contract_ranks(records, period_column="date")
        # No row dropped (deltas past the config count fall through the merge
        # unmatched, not here).
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["contract_rank"], "11")
        self.assertNotIn("contract_rank", records[0])  # originals untouched

    def test_unparseable_period_or_delivery_month_passes_through_unstamped(self):
        records = [
            {"date": "n/a", "delivery_month": "2024-08", "settle": 1},
            {"date": "2024-07-10", "delivery_month": "al2408", "settle": 2},
            {"date": "2024-07-10", "settle": 3},
        ]
        out = stamp_contract_ranks(records, period_column="date")
        self.assertEqual(len(out), 3)
        for row in out:
            self.assertNotIn("contract_rank", row)

    def test_preserves_input_order(self):
        records = [
            {"date": "2024-07-11", "delivery_month": "2024-07"},
            {"date": "2024-07-10", "delivery_month": "2024-07"},
        ]
        out = stamp_contract_ranks(records, period_column="date")
        self.assertEqual([r["date"] for r in out], ["2024-07-11", "2024-07-10"])

    def test_empty_input_is_empty(self):
        self.assertEqual(stamp_contract_ranks([], period_column="date"), [])


def _lme_cot_rows(date="2026-07-03", metal="aluminium") -> list[dict]:
    """The five per-category rows of one LME COT issue (the 2026-07-07 live file)."""
    base = {"date": date, "metal": metal, "published_at": "2026-07-07T05:01:32Z"}
    return [
        {**base, "category": "investment_firms_credit_institutions",
         "risk_reducing_long": 59283.96, "risk_reducing_short": 49956.37,
         "total_long": 476554.8, "total_short": 514731.18},
        {**base, "category": "investment_funds",
         "risk_reducing_long": 0, "risk_reducing_short": 5,
         "total_long": 173758.87, "total_short": 43003.64},
        {**base, "category": "other_financial_institutions",
         "risk_reducing_long": 4879, "risk_reducing_short": 5286,
         "total_long": 42467.95, "total_short": 20085.83},
        {**base, "category": "commercial_undertakings",
         "risk_reducing_long": 103195.99, "risk_reducing_short": 166701.76,
         "total_long": 233042.95, "total_short": 348671.21},
        {**base, "category": "compliance_operators",
         "risk_reducing_long": 0, "risk_reducing_short": 0,
         "total_long": 0, "total_short": 0},
    ]


class DeriveLmeCotSummaryTests(TestCase):
    def test_appends_one_summary_row_per_date_with_derived_metrics(self):
        rows = _lme_cot_rows()
        out = derive_lme_cot_summary(rows, period_column="date")
        # Originals pass through untouched, the summary is appended.
        self.assertEqual(out[:5], rows)
        self.assertEqual(len(out), 6)
        summary = out[5]
        self.assertEqual(summary["date"], "2026-07-03")
        self.assertEqual(summary["metal"], "aluminium")
        self.assertEqual(summary["category"], "market_summary")
        # Commercial = Commercial Undertakings risk-reducing long/short.
        self.assertEqual(summary["commercial_long"], 103195.99)
        self.assertEqual(summary["commercial_short"], 166701.76)
        self.assertAlmostEqual(summary["commercial_net"], -63505.77)
        # Investment funds = Investment Funds total long/short.
        self.assertEqual(summary["investment_funds_long"], 173758.87)
        self.assertEqual(summary["investment_funds_short"], 43003.64)
        self.assertAlmostEqual(summary["investment_funds_net"], 130755.23)
        # Open interest = the five categories' total longs summed.
        self.assertAlmostEqual(summary["open_interest"], 925824.57)

    def test_groups_by_date_and_metal(self):
        rows = _lme_cot_rows(date="2026-06-26") + _lme_cot_rows(date="2026-07-03")
        out = derive_lme_cot_summary(rows, period_column="date")
        summaries = [r for r in out if r["category"] == "market_summary"]
        self.assertEqual(
            [r["date"] for r in summaries], ["2026-06-26", "2026-07-03"]
        )

    def test_missing_category_nulls_open_interest_not_a_partial_sum(self):
        rows = [r for r in _lme_cot_rows() if r["category"] != "compliance_operators"]
        out = derive_lme_cot_summary(rows, period_column="date")
        summary = out[-1]
        self.assertIsNone(summary["open_interest"])
        # The per-category metrics still derive from the rows that exist.
        self.assertEqual(summary["commercial_long"], 103195.99)

    def test_null_side_propagates_to_net(self):
        rows = _lme_cot_rows()
        rows[3] = {**rows[3], "risk_reducing_short": None}
        out = derive_lme_cot_summary(rows, period_column="date")
        summary = out[-1]
        self.assertIsNone(summary["commercial_net"])
        self.assertEqual(summary["commercial_long"], 103195.99)
        self.assertIsNone(summary["commercial_short"])

    def test_empty_input_is_empty(self):
        self.assertEqual(derive_lme_cot_summary([], period_column="date"), [])
