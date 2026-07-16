from __future__ import annotations

from unittest import TestCase

from external_data.common.grain_catalog import (
    collect_unique_grain_targets,
    select_catalog_document,
)


DATASET_ID = "69328022455eba93435a27ce"
OTHER_DATASET_ID = "6985f24347c1ab09efdd7064"


def _catalog_document(catalog_id, dataset_id, info):
    return {
        "_id": catalog_id,
        "name": f"name of {catalog_id}",
        "process": {"datasetId": dataset_id, "grainId": "CHN"},
        "info": info,
    }


class SelectCatalogDocumentTest(TestCase):
    def test_returns_none_without_documents(self):
        self.assertIsNone(
            select_catalog_document(
                [],
                dataset_id=DATASET_ID,
                grain_id="CHN",
            )
        )

    def test_prefers_document_matching_target_dataset_id(self):
        # A reused grain id (e.g. CHN) maps to genuinely different catalog
        # variables; the target dataset_id must pick the right one.
        oecd = _catalog_document("Idx_OECD_CLI_CHN", DATASET_ID, {"source": "OECD"})
        steel = _catalog_document(
            "Com_Steel_CrudeSteel_Total_Prod_China_WS",
            OTHER_DATASET_ID,
            {"source": "World Steel"},
        )

        selected = select_catalog_document(
            [steel, oecd],
            dataset_id=DATASET_ID,
            grain_id="CHN",
        )

        self.assertEqual("Idx_OECD_CLI_CHN", selected["_id"])

    def test_collapses_metric_suffix_variants_to_base_catalog_id(self):
        info = {"source": "SHFE", "unit": "t"}
        documents = [
            _catalog_document("Com_SHFE_Cu.close", DATASET_ID, dict(info)),
            _catalog_document("Com_SHFE_Cu", DATASET_ID, dict(info)),
            _catalog_document("Com_SHFE_Cu.open", DATASET_ID, dict(info)),
        ]

        selected = select_catalog_document(
            documents,
            dataset_id=DATASET_ID,
            grain_id="VC",
        )

        self.assertEqual("Com_SHFE_Cu", selected["_id"])

    def test_falls_back_to_all_documents_when_dataset_id_never_matches(self):
        # The catalog datasetId legitimately diverges from the target
        # dataset_id for some grains; those documents must stay usable.
        documents = [
            _catalog_document(
                "Com_China_RefinedNickel_Import",
                OTHER_DATASET_ID,
                {"source": "Bloomberg"},
            ),
            _catalog_document(
                "Com_China_RefinedNickel_Import.close",
                OTHER_DATASET_ID,
                {"source": "Bloomberg"},
            ),
        ]

        selected = select_catalog_document(
            documents,
            dataset_id=DATASET_ID,
            grain_id="RNICIQTL_Index",
        )

        self.assertEqual("Com_China_RefinedNickel_Import", selected["_id"])

    def test_fails_loudly_on_conflicting_info_objects(self):
        documents = [
            _catalog_document("FRED_Building_Permits", OTHER_DATASET_ID, {"source": "FRED"}),
            _catalog_document("Com_Building_Permit_US", OTHER_DATASET_ID, {"source": "DOC"}),
        ]

        with self.assertRaises(ValueError) as raised:
            select_catalog_document(
                documents,
                dataset_id=DATASET_ID,
                grain_id="PERMIT",
            )

        self.assertIn("PERMIT", str(raised.exception))
        self.assertIn("distinct catalog info objects", str(raised.exception))


class CollectUniqueGrainTargetsTest(TestCase):
    def test_unions_every_category_without_duplicate_pairs(self):
        targets = collect_unique_grain_targets()

        self.assertTrue(targets)
        pairs = [
            (target["dataset_id"], target["grain_id"]) for target in targets
        ]
        self.assertEqual(len(pairs), len(set(pairs)))
        for target in targets:
            self.assertIn("description", target)
            self.assertIn("freq", target)
