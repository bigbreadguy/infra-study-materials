from __future__ import annotations

from unittest import TestCase

from external_data.common.scrape_request import (
    SCHEMA_VERSION,
    build_request_payload,
    clamp_date_range_to_ceiling,
    clamp_date_range_to_floor,
    clamp_period_to_floor,
    gcs_uri,
    merge_params,
    month_bounds_iso,
    month_window_range,
    recent_days_range_iso,
    request_object_name,
    resolve_year_month,
    result_object_name,
    split_date_range_yearly,
    year_month_from_iso,
)


class ResolveYearMonthTests(TestCase):
    def test_no_lookback_returns_same(self):
        self.assertEqual(resolve_year_month(2001, 1, 0), (2001, 1))

    def test_lookback_rolls_over_year(self):
        # January minus 2 months -> previous November.
        self.assertEqual(resolve_year_month(2001, 1, 2), (2000, 11))

    def test_lookback_within_year(self):
        self.assertEqual(resolve_year_month(2026, 6, 3), (2026, 3))

    def test_rejects_bad_month(self):
        with self.assertRaises(ValueError):
            resolve_year_month(2026, 13, 0)

    def test_rejects_negative_lookback(self):
        with self.assertRaises(ValueError):
            resolve_year_month(2026, 6, -1)


class ClampPeriodToFloorTests(TestCase):
    # floor used throughout: 2000-01.
    def test_range_fully_above_floor_unchanged(self):
        self.assertEqual(
            clamp_period_to_floor(2010, 3, 2010, 6, 2000, 1),
            (2010, 3, 2010, 6),
        )

    def test_start_below_floor_is_raised_end_untouched(self):
        # steel_scrap_domestic-style deep backfill: 1995 start raised to floor.
        self.assertEqual(
            clamp_period_to_floor(1995, 5, 2010, 6, 2000, 1),
            (2000, 1, 2010, 6),
        )

    def test_start_exactly_on_floor_unchanged(self):
        self.assertEqual(
            clamp_period_to_floor(2000, 1, 2005, 12, 2000, 1),
            (2000, 1, 2005, 12),
        )

    def test_whole_range_below_floor_returns_none(self):
        # Even end < floor: an explicit empty request, not an inverted range.
        self.assertIsNone(clamp_period_to_floor(1990, 1, 1999, 12, 2000, 1))

    def test_end_exactly_on_floor_collapses_to_floor_month(self):
        # end == floor is in range; start clamps up to it (single-month request).
        self.assertEqual(
            clamp_period_to_floor(1990, 1, 2000, 1, 2000, 1),
            (2000, 1, 2000, 1),
        )

    def test_compared_jointly_not_field_by_field(self):
        # Floor 2000-06: a 2000-03 start is below the floor as a joint period even
        # though its year equals the floor year -> raised to 2000-06, not left at
        # month 03.
        self.assertEqual(
            clamp_period_to_floor(2000, 3, 2001, 2, 2000, 6),
            (2000, 6, 2001, 2),
        )


class MonthBoundsIsoTests(TestCase):
    def test_31_day_month(self):
        self.assertEqual(month_bounds_iso(2024, 1), ("2024-01-01", "2024-01-31"))

    def test_30_day_month(self):
        self.assertEqual(month_bounds_iso(2024, 4), ("2024-04-01", "2024-04-30"))

    def test_leap_february(self):
        self.assertEqual(month_bounds_iso(2024, 2), ("2024-02-01", "2024-02-29"))

    def test_non_leap_february(self):
        self.assertEqual(month_bounds_iso(2023, 2), ("2023-02-01", "2023-02-28"))

    def test_zero_pads_single_digit_month(self):
        self.assertEqual(month_bounds_iso(2015, 9), ("2015-09-01", "2015-09-30"))

    def test_rejects_bad_month(self):
        with self.assertRaises(ValueError):
            month_bounds_iso(2024, 13)


class MonthWindowRangeTests(TestCase):
    def test_default_two_month_window_is_prev_through_current(self):
        # window=2 ending at 2026-06 -> [2026-05 .. 2026-06].
        self.assertEqual(month_window_range(2026, 6, 2), (2026, 5, 2026, 6))

    def test_single_month_window(self):
        self.assertEqual(month_window_range(2026, 6, 1), (2026, 6, 2026, 6))

    def test_window_rolls_over_year(self):
        # window=3 ending at 2026-01 -> [2025-11 .. 2026-01].
        self.assertEqual(month_window_range(2026, 1, 3), (2025, 11, 2026, 1))

    def test_window_floored_at_one(self):
        self.assertEqual(month_window_range(2026, 6, 0), (2026, 6, 2026, 6))


class YearMonthFromIsoTests(TestCase):
    def test_extracts_year_month(self):
        self.assertEqual(year_month_from_iso("2026-06-24"), (2026, 6))

    def test_zero_padded_month(self):
        self.assertEqual(year_month_from_iso("2015-09-01"), (2015, 9))

    def test_rejects_malformed_date(self):
        with self.assertRaises(ValueError):
            year_month_from_iso("2026-6-1")


class RecentDaysRangeIsoTests(TestCase):
    def test_seven_day_window_through_end(self):
        self.assertEqual(
            recent_days_range_iso("2026-06-23", 7), ("2026-06-17", "2026-06-23")
        )

    def test_single_day_window(self):
        self.assertEqual(
            recent_days_range_iso("2026-06-23", 1), ("2026-06-23", "2026-06-23")
        )

    def test_window_crosses_month_boundary(self):
        self.assertEqual(
            recent_days_range_iso("2026-03-02", 5), ("2026-02-26", "2026-03-02")
        )

    def test_window_floored_at_one(self):
        self.assertEqual(
            recent_days_range_iso("2026-06-23", 0), ("2026-06-23", "2026-06-23")
        )

    def test_rejects_malformed_end(self):
        with self.assertRaises(ValueError):
            recent_days_range_iso("2026-6-1", 7)


class ClampDateRangeToFloorTests(TestCase):
    # floor used throughout: 2015-01-01 (TRYUSD=X history start).
    FLOOR = "2015-01-01"

    def test_range_fully_above_floor_unchanged(self):
        self.assertEqual(
            clamp_date_range_to_floor("2020-01-01", "2020-12-31", self.FLOOR),
            ("2020-01-01", "2020-12-31"),
        )

    def test_start_below_floor_is_raised_end_untouched(self):
        self.assertEqual(
            clamp_date_range_to_floor("2010-06-01", "2020-01-01", self.FLOOR),
            ("2015-01-01", "2020-01-01"),
        )

    def test_start_exactly_on_floor_unchanged(self):
        self.assertEqual(
            clamp_date_range_to_floor("2015-01-01", "2018-05-05", self.FLOOR),
            ("2015-01-01", "2018-05-05"),
        )

    def test_whole_range_below_floor_returns_none(self):
        self.assertIsNone(
            clamp_date_range_to_floor("2010-01-01", "2014-12-31", self.FLOOR)
        )

    def test_end_exactly_on_floor_collapses_to_floor_day(self):
        # end == floor is in range; start clamps up to it (single-day request).
        self.assertEqual(
            clamp_date_range_to_floor("2010-01-01", "2015-01-01", self.FLOOR),
            ("2015-01-01", "2015-01-01"),
        )

    def test_rejects_malformed_date(self):
        for bad in ("2015-1-1", "2015/01/01", "20150101", "abc"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    clamp_date_range_to_floor(bad, "2020-01-01", self.FLOOR)


class ClampDateRangeToCeilingTests(TestCase):
    # ceiling used throughout: yesterday KST as seen by the 2026-07-14 backfill run
    # that requested end=today and hit shfe's not-yet-settled intraday kx file.
    CEILING = "2026-07-13"

    def test_range_fully_below_ceiling_unchanged(self):
        self.assertEqual(
            clamp_date_range_to_ceiling("2002-01-07", "2026-07-10", self.CEILING),
            ("2002-01-07", "2026-07-10"),
        )

    def test_end_above_ceiling_is_lowered_start_untouched(self):
        # The failing backfill's shape: end=today clamps down to yesterday.
        self.assertEqual(
            clamp_date_range_to_ceiling("2002-01-07", "2026-07-14", self.CEILING),
            ("2002-01-07", "2026-07-13"),
        )

    def test_end_exactly_on_ceiling_unchanged(self):
        self.assertEqual(
            clamp_date_range_to_ceiling("2026-07-01", "2026-07-13", self.CEILING),
            ("2026-07-01", "2026-07-13"),
        )

    def test_whole_range_above_ceiling_returns_none(self):
        # An explicit today-only (or future) request: nothing published to scrape.
        self.assertIsNone(
            clamp_date_range_to_ceiling("2026-07-14", "2026-07-14", self.CEILING)
        )

    def test_start_exactly_on_ceiling_collapses_to_ceiling_day(self):
        # start == ceiling is in range; end clamps down to it (single-day request).
        self.assertEqual(
            clamp_date_range_to_ceiling("2026-07-13", "2026-07-20", self.CEILING),
            ("2026-07-13", "2026-07-13"),
        )

    def test_rejects_malformed_date(self):
        for bad in ("2026-7-1", "2026/07/01", "20260701", "abc"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    clamp_date_range_to_ceiling(bad, "2026-07-01", self.CEILING)


class ResolveDateRangeBackfillClampTests(TestCase):
    """The DAG-side wiring of the ceiling clamp: an explicit backfill window's end
    is clamped down to yesterday (KST), and a window entirely after yesterday skips
    -- the 2026-07-14 shfe backfill regression (end=today reached the scraper and
    hit the not-yet-settled intraday kx file)."""

    def _resolve(self, conf):
        from unittest import mock

        import pendulum

        from external_data import scrape_external_data_pipeline as pipeline

        run_point = pendulum.datetime(2026, 7, 14, 15, 0, tz="Asia/Seoul")
        with mock.patch.object(pipeline, "_run_point_kst", return_value=run_point):
            return pipeline._resolve_date_range(conf)

    def test_explicit_end_today_clamps_to_yesterday(self):
        self.assertEqual(
            self._resolve({"start_date": "2002-01-07", "end_date": "2026-07-14"}),
            ("2002-01-07", "2026-07-13"),
        )

    def test_explicit_past_range_is_unchanged(self):
        self.assertEqual(
            self._resolve({"start_date": "2026-07-01", "end_date": "2026-07-10"}),
            ("2026-07-01", "2026-07-10"),
        )

    def test_today_only_backfill_skips(self):
        from airflow.exceptions import AirflowSkipException

        with self.assertRaises(AirflowSkipException):
            self._resolve({"start_date": "2026-07-14"})


class SplitDateRangeYearlyTests(TestCase):
    def test_single_year_window_is_one_chunk(self):
        # The daily-monitor case: never chunked, the input range comes back as-is.
        self.assertEqual(
            split_date_range_yearly("2026-07-01", "2026-07-07"),
            [("2026-07-01", "2026-07-07")],
        )

    def test_single_day_window(self):
        self.assertEqual(
            split_date_range_yearly("2026-07-07", "2026-07-07"),
            [("2026-07-07", "2026-07-07")],
        )

    def test_multi_year_backfill_splits_per_calendar_year(self):
        # shfe.futures_daily-style deep backfill: first chunk starts at the
        # requested start, interior years span 01-01..12-31, last chunk ends at
        # the requested end.
        self.assertEqual(
            split_date_range_yearly("2023-03-15", "2026-07-07"),
            [
                ("2023-03-15", "2023-12-31"),
                ("2024-01-01", "2024-12-31"),
                ("2025-01-01", "2025-12-31"),
                ("2026-01-01", "2026-07-07"),
            ],
        )

    def test_year_boundary_pair_is_two_chunks(self):
        self.assertEqual(
            split_date_range_yearly("2025-12-31", "2026-01-01"),
            [("2025-12-31", "2025-12-31"), ("2026-01-01", "2026-01-01")],
        )

    def test_chunks_are_contiguous_and_cover_the_range(self):
        chunks = split_date_range_yearly("2002-01-07", "2026-07-07")
        self.assertEqual(len(chunks), 25)
        self.assertEqual(chunks[0][0], "2002-01-07")
        self.assertEqual(chunks[-1][1], "2026-07-07")
        for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:]):
            self.assertEqual(prev_end[:4], str(int(next_start[:4]) - 1))
            self.assertEqual(prev_end[5:], "12-31")
            self.assertEqual(next_start[5:], "01-01")

    def test_rejects_inverted_range(self):
        with self.assertRaises(ValueError):
            split_date_range_yearly("2026-07-07", "2026-07-01")

    def test_rejects_malformed_date(self):
        for bad in ("2026-7-1", "2026/07/01", "20260701"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    split_date_range_yearly(bad, "2026-07-07")


class MergeParamsTests(TestCase):
    def test_conf_overrides_defaults(self):
        defaults = {"country": "일본", "country_code": "104"}
        merged = merge_params(defaults, {"country_code": "999"})
        self.assertEqual(merged["country_code"], "999")
        self.assertEqual(merged["country"], "일본")

    def test_none_conf_returns_copy(self):
        defaults = {"a": 1}
        merged = merge_params(defaults, None)
        self.assertEqual(merged, {"a": 1})
        self.assertIsNot(merged, defaults)


class BuildRequestPayloadTests(TestCase):
    def test_payload_shape(self):
        payload = build_request_payload("kosa.steel_scrap", {"year": 2001, "month": 1})
        self.assertEqual(payload["schema_version"], SCHEMA_VERSION)
        self.assertEqual(payload["recipe"], "kosa.steel_scrap")
        self.assertEqual(payload["params"]["year"], 2001)
        # output_uri is never embedded in the request.
        self.assertNotIn("output_uri", payload)

    def test_rejects_empty_recipe(self):
        with self.assertRaises(ValueError):
            build_request_payload("", {})


class ObjectPathTests(TestCase):
    def test_paths_keyed_by_run_id_and_recipe(self):
        self.assertEqual(
            request_object_name("run-1", "kosa.steel_scrap_import"),
            "scrape/requests/run-1/kosa.steel_scrap_import.json",
        )
        self.assertEqual(
            result_object_name("run-1", "kosa.steel_scrap_import"),
            "scrape/results/run-1/kosa.steel_scrap_import.json",
        )

    def test_recipes_of_one_run_do_not_collide(self):
        run_id = "run-1"
        a = request_object_name(run_id, "kosa.steel_scrap_import")
        b = request_object_name(run_id, "kosa.steel_scrap_domestic")
        self.assertNotEqual(a, b)

    def test_chunked_paths_carry_the_chunk_key(self):
        # A yearly-chunked backfill builds several requests per recipe; each
        # chunk's objects are additionally keyed by its year.
        self.assertEqual(
            request_object_name("run-1", "shfe.futures_daily", chunk="2015"),
            "scrape/requests/run-1/shfe.futures_daily.2015.json",
        )
        self.assertEqual(
            result_object_name("run-1", "shfe.futures_daily", chunk="2015"),
            "scrape/results/run-1/shfe.futures_daily.2015.json",
        )

    def test_chunks_of_one_recipe_do_not_collide(self):
        a = result_object_name("run-1", "shfe.futures_daily", chunk="2015")
        b = result_object_name("run-1", "shfe.futures_daily", chunk="2016")
        unchunked = result_object_name("run-1", "shfe.futures_daily")
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, unchunked)

    def test_none_chunk_keeps_the_unchunked_path(self):
        self.assertEqual(
            request_object_name("run-1", "shfe.futures_daily", chunk=None),
            request_object_name("run-1", "shfe.futures_daily"),
        )

    def test_gcs_uri(self):
        self.assertEqual(
            gcs_uri("dfml-dev-raw", "scrape/requests/run-1/kosa.steel_scrap_import.json"),
            "gs://dfml-dev-raw/scrape/requests/run-1/kosa.steel_scrap_import.json",
        )
