"""USDA PSD filter/load helper tests."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone

from external_data.common.usda_psd import (
    FILTER_ATTRIBUTE_DESCRIPTION,
    FILTER_COMMODITY_DESCRIPTION,
    FILTER_COUNTRY_CODE,
    FILTER_UNIT_ID,
    _row_matches_filter,
    default_gcs_prefix,
    iter_filtered_rows_from_zip,
)


def _zip_with_csv(*lines: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("psd_oilseeds.csv", "\n".join(lines))
    return buf.getvalue()


def test_default_gcs_prefix():
    assert default_gcs_prefix("2026-06-01") == (
        "project=external_data/source=usda_psd/dataset=oilseeds"
        "/ingest_date=2026-06-01"
    )


def test_row_matches_filter():
    match = {
        "Commodity_Description": FILTER_COMMODITY_DESCRIPTION,
        "Country_Code": FILTER_COUNTRY_CODE,
        "Attribute_Description": FILTER_ATTRIBUTE_DESCRIPTION,
        "Unit_ID": FILTER_UNIT_ID,
    }
    assert _row_matches_filter(match) is True
    assert _row_matches_filter({**match, "Country_Code": "BR"}) is False
    assert _row_matches_filter(
        {**match, "Commodity_Description": "Oilseed, Soybean (Local)"}
    ) is False


def test_row_matches_filter_rejects_other_commodity():
    row = {
        "Commodity_Description": "Oilseed, Rapeseed",
        "Country_Code": FILTER_COUNTRY_CODE,
        "Attribute_Description": FILTER_ATTRIBUTE_DESCRIPTION,
        "Unit_ID": FILTER_UNIT_ID,
    }
    assert _row_matches_filter(row) is False


def test_iter_filtered_rows_from_zip():
    header = (
        "Commodity_Code,Commodity_Description,Country_Code,Country_Name,"
        "Market_Year,Calendar_Year,Month,Attribute_ID,Attribute_Description,"
        "Unit_ID,Unit_Description,Value"
    )
    us_row = (
        '2222000,"Oilseed, Soybean",US,"United States",2025,2026,05,028,'
        '"Production",08,"(1000 MT)",115989.0000'
    )
    other_row = (
        '2222000,"Oilseed, Soybean",AG,"Algeria",2025,2026,05,028,'
        '"Production",08,"(1000 MT)",0.0000'
    )
    zip_bytes = _zip_with_csv(header, us_row, other_row)
    ingested = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = list(iter_filtered_rows_from_zip(zip_bytes, ingested_at=ingested))
    assert len(rows) == 1
    assert rows[0]["country_code"] == "US"
    assert rows[0]["market_year"] == 2025
    assert rows[0]["value"] == 115989.0
    assert "source_zip_uri" not in rows[0]
