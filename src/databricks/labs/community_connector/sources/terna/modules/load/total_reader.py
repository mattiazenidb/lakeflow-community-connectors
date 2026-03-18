"""Reader for the total (load) table (Terna load API).

Year is mandatory; region, province, sector, activity, and class are optional filters.
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

TOTAL_PATH = "/load/v2.0/total"


class TotalStatReader:
    """Reads total (load statistics) data from the Terna Public API for a single year."""

    TOTAL_STAT_KEY = "total_stat"

    TOTAL_STAT_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("region", StringType(), True),
            StructField("province", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("activity", StringType(), True),
            StructField("class", StringType(), True),
            StructField("consumption_GWh", StringType(), True),
        ]
    )

    TOTAL_STAT_METADATA = {
        "primary_keys": ["year", "region", "province", "sector", "activity", "class"],
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
        """Read total (load stat) records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        if year_str is None:
            raise ValueError("total_stat requires 'year'")

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"total_stat: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        for opt in ("region", "province", "sector", "activity", "class"):
            val = table_options.get(opt)
            if val is not None:
                params[opt] = val

        logger.debug("Querying total_stat for year=%s params=%s", year, params)
        resp = self._client.request("GET", TOTAL_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for total_stat: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get("total")
        records: list[dict] = data if isinstance(data, list) else []

        return iter(records), {"cursor": str(year)}
