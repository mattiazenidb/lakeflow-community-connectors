"""Reader for the services_sector table (Terna load API).

Year is mandatory; region, province, activity, and class are optional filters.
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

SERVICES_SECTOR_PATH = "/load/v2.0/services-sector"


class ServicesSectorReader:
    """Reads services_sector data from the Terna Public API for a single year."""

    SERVICES_SECTOR_KEY = "services_sector"

    SERVICES_SECTOR_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("region", StringType(), True),
            StructField("province", StringType(), True),
            StructField("activity", StringType(), True),
            StructField("class", StringType(), True),
            StructField("consumption_GWh", StringType(), True),
        ]
    )

    SERVICES_SECTOR_METADATA = {
        "primary_keys": ["year", "region", "province", "activity", "class"],
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
        """Read services_sector records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        if year_str is None:
            raise ValueError("services_sector requires 'year'")

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"services_sector: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        for opt in ("region", "province", "activity", "class"):
            val = table_options.get(opt)
            if val is not None:
                params[opt] = val

        logger.debug("Querying services_sector for year=%s params=%s", year, params)
        resp = self._client.request("GET", SERVICES_SECTOR_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for services_sector: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(self.SERVICES_SECTOR_KEY)
        records: list[dict] = data if isinstance(data, list) else []

        return iter(records), {"cursor": str(year)}
