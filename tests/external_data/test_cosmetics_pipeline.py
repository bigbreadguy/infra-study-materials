"""Mode-branching truth table, param validation, staging, and object naming.

Imports the DAG module (pure module-level helpers) plus the staging glue. No Airflow
runtime / BigQuery — the mode helpers and staging are pure.
"""

from __future__ import annotations

import json

import pytest

from external_data import cosmetics_pipeline as dag_mod
from external_data.common.cosmetics_bigquery import (
    LINEAGE_ENVELOPE_URI,
    LINEAGE_SCRAPED_AT,
    LINEAGE_SNAPSHOT_ID,
    cosmetics_staging_ndjson,
)
from external_data.common.cosmetics_schema import RECIPES


# --- mode resolution truth table -------------------------------------------


def test_scheduled_default_is_scrape():
    assert dag_mod.resolve_mode_value({}, "scheduled") == "scrape_recipe"


def test_any_backfill_param_selects_backfill_on_manual_run():
    for params in ({"backfill_start": "2026-07-01"}, {"backfill_end": "2026-07-07"},
                   {"backfill_snapshots": ["20260707/T092542"]}):
        assert dag_mod.resolve_mode_value(params, "manual") == "backfill_snapshots"


def test_backfill_params_on_scheduled_run_fail_loudly():
    with pytest.raises(ValueError):
        dag_mod.resolve_mode_value({"backfill_start": "2026-07-01"}, "scheduled")


def test_scrape_knobs_do_not_trigger_backfill():
    assert not dag_mod.backfill_requested({"recipes": ["lottedfs.best_rankings"], "max_details": 5})
    assert dag_mod.resolve_mode_value({"recipes": ["lottedfs.best_rankings"]}, "manual") == "scrape_recipe"


# --- param validation ------------------------------------------------------


def test_selected_recipes_subset_and_unknown():
    assert dag_mod.selected_recipes({}) == list(RECIPES)
    assert dag_mod.selected_recipes({"recipes": ["ssgdfs.best_rankings"]}) == ["ssgdfs.best_rankings"]
    # preserves canonical order regardless of input order
    got = dag_mod.selected_recipes({"recipes": ["naver_best.best_rankings", "lottedfs.best_rankings"]})
    assert got == [r for r in RECIPES if r in got]
    with pytest.raises(ValueError):
        dag_mod.selected_recipes({"recipes": ["tiktok.best_rankings"]})


def test_selected_sources_subset_and_unknown():
    assert set(dag_mod.selected_sources({})) == set(dag_mod.cb.BACKFILL_SOURCES)
    assert dag_mod.selected_sources({"backfill_sources": ["lottedfs"]}) == ["lottedfs"]
    with pytest.raises(ValueError):
        dag_mod.selected_sources({"backfill_sources": ["festa"]})


# --- object naming ---------------------------------------------------------


def test_snapshot_id_from_result_object():
    assert dag_mod.snapshot_id_from_result_object(
        "scrape/results/manual__2026-07-07T00:00:00/lottedfs.best_rankings.json"
    ) == "manual__2026-07-07T00:00:00"
    assert dag_mod.snapshot_id_from_result_object(
        "scrape/results/cnp_backfill/20260707T092542/ssgdfs.best_rankings.json"
    ) == "cnp_backfill/20260707T092542"
    with pytest.raises(ValueError):
        dag_mod.snapshot_id_from_result_object("scrape/results/whatever")


def test_staging_object_name_is_run_scoped_and_slugged():
    name = dag_mod._staging_object_name("run-1", "cnp_backfill/20260707T092542", "ssgdfs.best_rankings")
    assert name.startswith("scrape/staging/cosmetics/run-1/")
    assert name.endswith(".ndjson")
    # the snapshot id's '/' and the recipe's '.' are slugged out of the filename
    filename = name.rsplit("/", 1)[-1]
    assert "/" not in filename and filename == "cnp_backfill_20260707T092542_ssgdfs_best_rankings.ndjson"
    assert name.count("/") == 4


# --- staging (envelope -> lineage-stamped NDJSON) --------------------------


def _envelope(status="success", data=None):
    return {
        "schema_version": "1", "recipe": "ssgdfs.best_rankings",
        "scraped_at": "2026-07-07T00:25:42Z", "status": status,
        "data": data if data is not None else [], "error": None,
    }


def test_staging_stamps_lineage_and_wraps_rows():
    env = _envelope(data=[{"source": "ssgdfs", "rank": 1, "item_cd": "X"}])
    out = cosmetics_staging_ndjson(env, snapshot_id="cnp_backfill/20260707T092542",
                                   envelope_uri="gs://b/scrape/results/cnp_backfill/20260707T092542/ssgdfs.best_rankings.json")
    (line,) = out.splitlines()
    row = json.loads(line)["row"]
    assert row["source"] == "ssgdfs" and row["item_cd"] == "X"
    assert row[LINEAGE_SNAPSHOT_ID] == "cnp_backfill/20260707T092542"
    assert row[LINEAGE_SCRAPED_AT] == "2026-07-07T00:25:42Z"
    assert row[LINEAGE_ENVELOPE_URI].endswith("ssgdfs.best_rankings.json")


def test_staging_empty_for_failed_or_empty_envelope():
    assert cosmetics_staging_ndjson(_envelope(status="failed", data=[{"a": 1}]),
                                    snapshot_id="r", envelope_uri="gs://b/x.json") == ""
    assert cosmetics_staging_ndjson(_envelope(data=[]),
                                    snapshot_id="r", envelope_uri="gs://b/x.json") == ""


def test_staging_requires_lineage_args():
    env = _envelope(data=[{"source": "ssgdfs"}])
    with pytest.raises(ValueError):
        cosmetics_staging_ndjson(env, snapshot_id="", envelope_uri="gs://b/x.json")
    with pytest.raises(ValueError):
        cosmetics_staging_ndjson(env, snapshot_id="r", envelope_uri="")


# --- DAG structure ---------------------------------------------------------


def test_dag_wired_branch_to_load():
    dag = dag_mod.dag
    assert dag.dag_id == "external_data__cosmetics"
    assert set(t.task_id for t in dag.tasks) == {
        "resolve_mode", "scrape_recipe", "backfill_snapshots", "load_to_bigquery"}
    load = dag.get_task("load_to_bigquery")
    assert {t for t in load.upstream_task_ids} == {"scrape_recipe", "backfill_snapshots"}
    assert load.trigger_rule == "none_failed_min_one_success"
