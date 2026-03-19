"""Tests for the industry_sector table reader."""

from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.terna.terna import TernaLakeflowConnect
from tests.unit.sources.test_utils import load_config

import logging

logger = logging.getLogger(__name__)

TABLE = "industry_sector"


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

    with pytest.raises(ValueError, match="requires 'year'"):
        connector.read_table(TABLE, None, {})


def test_terna_invalid_year_format():
    """Non-integer year raises ValueError."""
    connector = _get_connector()

    with pytest.raises(ValueError, match="'year' must be an integer"):
        connector.read_table(TABLE, None, {"year": "abc"})


def test_terna_year_beyond_history_limit():
    """Year older than 5 solar years raises ValueError."""
    connector = _get_connector()

    with pytest.raises(ValueError, match="'year' must be within the last 5 solar years"):
        connector.read_table(TABLE, None, {"year": "2018"})


def test_terna_read_single_year():
    """Reading a known historical year returns data."""
    connector = _get_connector()

    records_iter, offset = connector.read_table(TABLE, None, {"year": "2023"})
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    assert offset.get("cursor") == "2023"


def test_terna_with_region_filter():
    """Reading with a region filter returns data for that region."""
    connector = _get_connector()

    records_iter, offset = connector.read_table(
        TABLE, None, {"year": "2023", "region": "Lombardia"}
    )
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert offset.get("cursor") == "2023"
    for record in records:
        assert record.get("region") == "Lombardia"


def test_terna_cursor_format():
    """Cursor is the year string."""
    connector = _get_connector()

    records_iter, offset = connector.read_table(TABLE, None, {"year": "2023"})
    list(records_iter)

    assert offset.get("cursor") == "2023"
