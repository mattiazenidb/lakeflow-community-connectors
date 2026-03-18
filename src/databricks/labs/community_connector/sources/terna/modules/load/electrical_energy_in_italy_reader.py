"""Reader for the electrical_energy_in_italy table (Terna load API).

Year is mandatory; month is an optional filter.
"""

import logging
from datetime import datetime, timezone
from typing import Iterator

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.sources.terna.utils.terna_api_client import (
    TernaApiClient,
)

logger = logging.getLogger(__name__)

TERNA_MAX_HISTORY_SOLAR_YEARS = 5

ELECTRICAL_ENERGY_IN_ITALY_PATH = "/load/v2.0/electrical-energy-in-italy"


class ElectricalEnergyInItalyReader:
    """Reads electrical_energy_in_italy data from the Terna Public API for a single year."""

    ELECTRICAL_ENERGY_IN_ITALY_KEY = "electrical_energy_in_italy"

    ELECTRICAL_ENERGY_IN_ITALY_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("demand_GWh", StringType(), True),
        ]
    )

    ELECTRICAL_ENERGY_IN_ITALY_METADATA = {
        "primary_keys": ["year", "month"],
        "cursor_field": "year",
        "ingestion_type": "append",
    }

    def __init__(self, client: TernaApiClient) -> None:
        self._client = client

    def read(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read electrical_energy_in_italy records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        if year_str is None:
            raise ValueError("electrical_energy_in_italy requires 'year'")

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"electrical_energy_in_italy: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        raw_month = table_options.get("month")
        if raw_month is not None:
            params["month"] = raw_month

        logger.debug("Querying electrical_energy_in_italy for year=%s params=%s", year, params)
        resp = self._client.request("GET", ELECTRICAL_ENERGY_IN_ITALY_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for electrical_energy_in_italy: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(self.ELECTRICAL_ENERGY_IN_ITALY_KEY)
        records: list[dict] = data if isinstance(data, list) else []

        return iter(records), {"cursor": str(year)}
