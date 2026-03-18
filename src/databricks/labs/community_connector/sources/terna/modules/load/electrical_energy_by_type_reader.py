"""Reader for the electrical_energy_by_type table (Terna load API).

Year is mandatory; type is an optional restricted-list filter.
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

ELECTRICAL_ENERGY_BY_TYPE_PATH = "/load/v2.0/electrical-energy-by-type"


class ElectricalEnergyByTypeReader:
    """Reads electrical_energy_by_type data from the Terna Public API for a single year."""

    ELECTRICAL_ENERGY_BY_TYPE_TYPES = [
        "Fonti rinnovabili",
        "Fonti tradizionali",
        "Saldo import/export",
    ]

    ELECTRICAL_ENERGY_BY_TYPE_KEY = "electrical_energy_by_type"

    ELECTRICAL_ENERGY_BY_TYPE_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("region", StringType(), True),
            StructField("type", StringType(), True),
            StructField("demand_GWh", StringType(), True),
        ]
    )

    ELECTRICAL_ENERGY_BY_TYPE_METADATA = {
        "primary_keys": ["year", "region", "type"],
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
        """Read electrical_energy_by_type records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        if year_str is None:
            raise ValueError("electrical_energy_by_type requires 'year'")

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"electrical_energy_by_type: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        raw_type = table_options.get("type")
        if raw_type is not None:
            if raw_type not in self.ELECTRICAL_ENERGY_BY_TYPE_TYPES:
                raise ValueError(
                    f"Terna connector: Invalid type value '{raw_type}'. "
                    f"Must be one of {', '.join(self.ELECTRICAL_ENERGY_BY_TYPE_TYPES)}"
                )
            params["type"] = raw_type

        logger.debug("Querying electrical_energy_by_type for year=%s params=%s", year, params)
        resp = self._client.request("GET", ELECTRICAL_ENERGY_BY_TYPE_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for electrical_energy_by_type: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(self.ELECTRICAL_ENERGY_BY_TYPE_KEY)
        records: list[dict] = data if isinstance(data, list) else []

        return iter(records), {"cursor": str(year)}
