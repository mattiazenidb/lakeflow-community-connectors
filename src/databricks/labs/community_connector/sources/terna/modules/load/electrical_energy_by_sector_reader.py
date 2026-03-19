"""Reader for the electrical_energy_by_sector table (Terna load API).

Year is mandatory; region, province, and sector are optional filters.
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

ELECTRICAL_ENERGY_BY_SECTOR_PATH = "/load/v2.0/electrical-energy-by-sector"


class ElectricalEnergyBySectorReader:
    """Reads electrical_energy_by_sector data from the Terna Public API for a single year."""

    ELECTRICAL_ENERGY_BY_SECTOR_SECTORS = [
        "Agricoltura",
        "Domestico",
        "Industria",
        "Servizi",
    ]

    ELECTRICAL_ENERGY_BY_SECTOR_KEY = "electrical_energy_by_sector"

    ELECTRICAL_ENERGY_BY_SECTOR_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("region", StringType(), True),
            StructField("province", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("consumption_GWh", StringType(), True),
        ]
    )

    ELECTRICAL_ENERGY_BY_SECTOR_METADATA = {
        "primary_keys": ["year", "region", "province", "sector"],
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
        """Read electrical_energy_by_sector records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        if year_str is None:
            raise ValueError("electrical_energy_by_sector requires 'year'")

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"electrical_energy_by_sector: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        raw_sector = table_options.get("sector")
        if raw_sector is not None:
            if raw_sector not in self.ELECTRICAL_ENERGY_BY_SECTOR_SECTORS:
                raise ValueError(
                    f"Terna connector: Invalid sector value '{raw_sector}'. "
                    f"Must be one of {', '.join(self.ELECTRICAL_ENERGY_BY_SECTOR_SECTORS)}"
                )
            params["sector"] = raw_sector

        raw_region = table_options.get("region")
        if raw_region is not None:
            params["region"] = raw_region

        raw_province = table_options.get("province")
        if raw_province is not None:
            params["province"] = raw_province

        logger.debug("Querying electrical_energy_by_sector for year=%s params=%s", year, params)
        resp = self._client.request("GET", ELECTRICAL_ENERGY_BY_SECTOR_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for electrical_energy_by_sector: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(self.ELECTRICAL_ENERGY_BY_SECTOR_KEY)
        records: list[dict] = data if isinstance(data, list) else []

        return iter(records), {"cursor": str(year)}
