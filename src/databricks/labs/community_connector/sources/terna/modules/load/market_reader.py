"""Reader for the market (load) table (Terna load API).

Year is mandatory; region, province, and Market are optional filters.
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

MARKET_PATH = "/load/v2.0/market"


class MarketStatReader:
    """Reads market (load statistics) data from the Terna Public API for a single year."""

    MARKET_STAT_MARKETS = [
        "Autoconsumo",
        "Mercato libero",
        "Mercato tutelato",
    ]

    MARKET_STAT_KEY = "market_stat"

    MARKET_STAT_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("region", StringType(), True),
            StructField("province", StringType(), True),
            StructField("market", StringType(), True),
            StructField("consumption_GWh", StringType(), True),
        ]
    )

    MARKET_STAT_METADATA = {
        "primary_keys": ["year", "region", "province", "market"],
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
        """Read market (load stat) records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        if year_str is None:
            raise ValueError("market_stat requires 'year'")

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"market_stat: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        raw_market = table_options.get("market")
        if raw_market is not None:
            if raw_market not in self.MARKET_STAT_MARKETS:
                raise ValueError(
                    f"Terna connector: Invalid market value '{raw_market}'. "
                    f"Must be one of {', '.join(self.MARKET_STAT_MARKETS)}"
                )
            params["Market"] = raw_market

        raw_region = table_options.get("region")
        if raw_region is not None:
            params["region"] = raw_region

        raw_province = table_options.get("province")
        if raw_province is not None:
            params["province"] = raw_province

        logger.debug("Querying market_stat for year=%s params=%s", year, params)
        resp = self._client.request("GET", MARKET_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for market_stat: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get("market")
        records: list[dict] = data if isinstance(data, list) else []

        return iter(records), {"cursor": str(year)}
