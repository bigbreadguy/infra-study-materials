from __future__ import annotations

import unittest

from external_data.common.dpanda_bloomberg_ingest import (
    CATEGORIES,
    materials_recipe,
)
from external_data.common.materials_metrics import (
    config_path,
    grain_targets_from_config,
)


class GrainTargetFilesTest(unittest.TestCase):
    """Grain targets now derive from the materials configs.

    The dpanda Bloomberg ``dpanda_bloomberg.<category>.json`` files are the single
    source of truth for grain identity, description, and freq; these tests guard
    the invariants the standalone grain catalog used to enforce.
    """

    def _targets_by_category(self) -> dict[str, list[dict]]:
        return {
            category: grain_targets_from_config(materials_recipe(category))
            for category in CATEGORIES
        }

    def test_every_category_has_a_materials_config_with_grains(self):
        for category in CATEGORIES:
            with self.subTest(category=category):
                self.assertTrue(
                    config_path(materials_recipe(category)).is_file(),
                    f"category {category} is registered but has no "
                    f"{materials_recipe(category)}.json",
                )
                self.assertGreater(
                    len(grain_targets_from_config(materials_recipe(category))),
                    0,
                    f"category {category} config yields no grain targets",
                )

    def test_each_grain_target_carries_the_expected_keys(self):
        for category, targets in self._targets_by_category().items():
            for target in targets:
                with self.subTest(category=category, target=target["grain_id"]):
                    self.assertEqual(
                        {"dataset_id", "grain_id", "description", "freq"},
                        set(target),
                    )

    def test_grain_pairs_are_unique_across_all_categories(self):
        """Disjoint pairs prevent concurrent category DAG double-inserts.

        Each category config feeds a DAG that merges into the same BigQuery
        tables, and max_active_runs=1 only serializes runs within one DAG.
        Grain identity is the (dataset_id, grain_id) pair -- the same grain_id
        legitimately recurs across datasets -- but one pair present in two
        category configs could merge concurrently and double-insert the same
        merge key through MERGE snapshot isolation.
        """
        first_seen_in: dict[tuple[str, str], str] = {}
        duplicates = []
        for category, targets in self._targets_by_category().items():
            for target in targets:
                pair = (target["dataset_id"], target["grain_id"])
                if pair in first_seen_in:
                    duplicates.append(
                        f"{pair} in {category} already defined in "
                        f"{first_seen_in[pair]}"
                    )
                else:
                    first_seen_in[pair] = category

        self.assertEqual(
            [],
            duplicates,
            "each dataset_id and grain_id pair must appear in exactly one "
            "category config: " + "; ".join(duplicates),
        )

    def test_grains_sharing_a_grain_id_carry_distinct_descriptions(self):
        """Same-named grains across datasets must stay tellable apart.

        Metric names derive from the grain_id, so duplicated grain names ship
        identical downstream metric names; the curated description flowing into
        dim_grains is what distinguishes them for consumers.
        """
        entries_by_grain_id: dict[str, list[dict]] = {}
        for targets in self._targets_by_category().values():
            for target in targets:
                entries_by_grain_id.setdefault(
                    target["grain_id"], []
                ).append(target)

        for grain_id, entries in entries_by_grain_id.items():
            if len(entries) < 2:
                continue
            with self.subTest(grain_id=grain_id):
                descriptions = [entry["description"] for entry in entries]
                self.assertEqual(
                    len(set(descriptions)),
                    len(descriptions),
                    f"targets named {grain_id} span several datasets and "
                    "are only tellable apart by description",
                )
