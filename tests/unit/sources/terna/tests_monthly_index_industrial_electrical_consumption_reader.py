"""Tests for the monthly_index_industrial_electrical_consumption table reader."""

from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.terna.terna import TernaLakeflowConnect
from tests.unit.sources.test_utils import load_config

import logging

logger = logging.getLogger(__name__)

TABLE = "monthly_index_industrial_electrical_consumption"


def _get_connector():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")
    return TernaLakeflowConnect(config)


def test_terna_init_raises_without_credentials():
    """Initializing without client_id or client_secret raises ValueError."""
    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({})

    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({"client_id": "x"})

    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({"client_secret": "y"})


def test_terna_missing_year():
    """When year is missing, connector raises ValueError."""
    connector = _get_connector()
    table_options = {"month": "11"}

    with pytest.raises(ValueError, match="requires 'year'"):
        connector.read_table(TABLE, None, table_options)


def test_terna_missing_month():
    """When month is missing, connector raises ValueError."""
    connector = _get_connector()
    table_options = {"year": "2024"}

    with pytest.raises(ValueError, match="requires 'month'"):
        connector.read_table(TABLE, None, table_options)


def test_terna_missing_both():
    """When both year and month are missing, connector raises ValueError."""
    connector = _get_connector()

    with pytest.raises(ValueError, match="requires 'year'"):
        connector.read_table(TABLE, None, {})


def test_terna_invalid_year_format():
    """Non-integer year raises ValueError."""
    connector = _get_connector()
    table_options = {"year": "abc", "month": "11"}

    with pytest.raises(ValueError, match="'year' must be an integer"):
        connector.read_table(TABLE, None, table_options)


def test_terna_invalid_month_format():
    """Non-integer month raises ValueError."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "abc"}

    with pytest.raises(ValueError, match="'month' must be an integer"):
        connector.read_table(TABLE, None, table_options)


def test_terna_month_out_of_range():
    """Month outside 1-12 raises ValueError."""
    connector = _get_connector()

    with pytest.raises(ValueError, match="'month' must be between 1 and 12"):
        connector.read_table(TABLE, None, {"year": "2024", "month": "13"})

    with pytest.raises(ValueError, match="'month' must be between 1 and 12"):
        connector.read_table(TABLE, None, {"year": "2024", "month": "0"})


def test_terna_year_beyond_history_limit():
    """Year older than 5 solar years raises ValueError."""
    connector = _get_connector()
    table_options = {"year": "2020", "month": "11"}

    with pytest.raises(ValueError, match="'year' must be within the last 5 solar years"):
        connector.read_table(TABLE, None, table_options)


def test_terna_invalid_sector():
    """An invalid sector value raises ValueError."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "11", "sectors": "INVALID_SECTOR"}

    with pytest.raises(ValueError, match="Invalid sector value INVALID_SECTOR"):
        connector.read_table(TABLE, None, table_options)


def test_terna_invalid_tension_type():
    """An invalid tension_type value raises ValueError."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "11", "tension_types": "BT"}

    with pytest.raises(ValueError, match="Invalid tension_type value BT"):
        connector.read_table(TABLE, None, table_options)


def test_terna_read_single_month():
    """Reading a known historical month returns data."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "11"}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    assert offset.get("cursor") == "2024/11"


def test_terna_with_sector_filter():
    """Reading with a sector filter returns only that sector."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "11", "sectors": "ALIMENTARE"}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    for record in records:
        assert record.get("sector") == "ALIMENTARE"


def test_terna_with_tension_type_filter():
    """Reading with a tension_type filter returns only that tension type."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "11", "tension_types": "AT"}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    for record in records:
        assert record.get("tension_type") == "AT"


def test_terna_with_sector_and_tension_type():
    """Reading with both sector and tension_type filters completes without error."""
    connector = _get_connector()
    table_options = {
        "year": "2024",
        "month": "11",
        "sectors": "CHIMICA",
        "tension_types": "MT",
    }

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert offset.get("cursor") == "2024/11"
    for record in records:
        assert record.get("sector") == "CHIMICA"
        assert record.get("tension_type") == "MT"


def test_terna_cursor_format():
    """Cursor is formatted as year/month (zero-padded)."""
    connector = _get_connector()
    table_options = {"year": "2024", "month": "2"}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    list(records_iter)

    assert offset.get("cursor") == "2024/02"
