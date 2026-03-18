"""Reader for the demand_coverage_by_source table (Terna load API).

This API uses a year query param (not dateFrom/dateTo like other load APIs).
Year is mandatory; region and source are optional filters.
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

DEMAND_COVERAGE_BY_SOURCE_PATH = "/load/v2.0/demand-coverage-by-source"


class DemandCoverageBySourceReader:
    """Reads demand_coverage_by_source data from the Terna Public API for a single year."""

    DEMAND_COVERAGE_BY_SOURCE_SOURCES = [
        "Bioenergie",
        "Eolico",
        "Fotovoltaico",
        "Geotermoelettrico",
        "Idrico rinnovabile",
        "Idrico tradizionale",
        "Saldo import/export",
    ]

    DEMAND_COVERAGE_BY_SOURCE_KEY = "demand_coverage_by_source"

    DEMAND_COVERAGE_BY_SOURCE_SCHEMA = StructType(
        [
            StructField("year", StringType(), True),
            StructField("region", StringType(), True),
            StructField("source", StringType(), True),
            StructField("coverage_GWh", StringType(), True),
        ]
    )

    DEMAND_COVERAGE_BY_SOURCE_METADATA = {
        "primary_keys": ["year", "region", "source"],
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
        """Read demand_coverage_by_source records for a single year."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")

        if year_str is None:
            raise ValueError(
                "demand_coverage_by_source requires 'year'"
            )

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"demand_coverage_by_source: 'year' must be an integer, got '{year_str}'"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

        params: dict[str, str] = {"year": str(year)}

        raw_source = table_options.get("source")
        if raw_source is not None:
            if raw_source not in self.DEMAND_COVERAGE_BY_SOURCE_SOURCES:
                raise ValueError(
                    f"Terna connector: Invalid source value '{raw_source}'. "
                    f"Must be one of {', '.join(self.DEMAND_COVERAGE_BY_SOURCE_SOURCES)}"
                )
            params["source"] = raw_source

        raw_region = table_options.get("region")
        if raw_region is not None:
            params["region"] = raw_region

        logger.debug("Querying demand_coverage_by_source for year=%s params=%s", year, params)
        resp = self._client.request(
            "GET",
            DEMAND_COVERAGE_BY_SOURCE_PATH,
            params=params,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for demand_coverage_by_source: "
                f"{resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(self.DEMAND_COVERAGE_BY_SOURCE_KEY)

        records: list[dict] = data if isinstance(data, list) else []

        cursor = str(year)
        return iter(records), {"cursor": cursor}
