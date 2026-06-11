from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.grain_targets import (
    CONFIG_DIR,
    list_grain_target_categories,
    parse_grain_targets,
)


class GrainTargetFilesTest(unittest.TestCase):
    def _grain_target_files(self):
        # Config files are gitignored and local-only, so their absence is a
        # fresh clone, not a failure.
        files = sorted(CONFIG_DIR.glob("*.json"))
        if not files:
            self.skipTest(
                f"no grain target config files in {CONFIG_DIR}; they are "
                "gitignored and local-only"
            )
        return files

    def test_each_grain_target_file_passes_validation(self):
        for config_file in self._grain_target_files():
            with self.subTest(config_file=config_file.name):
                targets = parse_grain_targets(
                    config_file.read_text(encoding="utf-8"),
                    source=f"grain targets file {config_file.name}",
                )

                self.assertGreater(len(targets), 0)

    def test_all_local_targets_are_enabled_with_expected_keys(self):
        for config_file in self._grain_target_files():
            with self.subTest(config_file=config_file.name):
                raw_entries = json.loads(config_file.read_text(encoding="utf-8"))
                targets = parse_grain_targets(
                    config_file.read_text(encoding="utf-8"),
                    source=f"grain targets file {config_file.name}",
                )

                # parse_grain_targets drops disabled entries silently; local
                # config should not carry disabled grains, or a run quietly
                # maps over fewer grains than the file suggests.
                self.assertEqual(len(targets), len(raw_entries))
                for target in targets:
                    self.assertEqual(
                        {"dataset_id", "grain_id", "description", "freq"},
                        set(target),
                    )

    def test_grain_ids_are_unique_across_all_grain_target_files(self):
        """Disjoint grain_ids prevent concurrent category DAG double-inserts.

        Each grain target file feeds a DAG that merges into the same BigQuery
        tables, and max_active_runs=1 only serializes runs within one DAG. A
        grain_id present in two files could therefore merge concurrently and
        double-insert the same merge key through MERGE snapshot isolation.
        Disabled entries count too: flipping enabled back on must not be able
        to introduce a duplicate.
        """
        first_seen_in: dict[str, str] = {}
        duplicates = []
        for config_file in self._grain_target_files():
            for entry in json.loads(config_file.read_text(encoding="utf-8")):
                grain_id = entry["grain_id"]
                if grain_id in first_seen_in:
                    duplicates.append(
                        f"{grain_id} in {config_file.name} "
                        f"already defined in {first_seen_in[grain_id]}"
                    )
                else:
                    first_seen_in[grain_id] = config_file.name

        self.assertEqual(
            [],
            duplicates,
            "grain_id must appear in exactly one grain target file: "
            + "; ".join(duplicates),
        )

    def test_discovered_categories_match_grain_target_file_stems(self):
        expected = sorted(path.stem for path in self._grain_target_files())

        self.assertEqual(expected, list_grain_target_categories())
