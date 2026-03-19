"""Terna Lakeflow community connector.

Ingests Italian electricity system data (load, generation, transmission)
from the Terna Public API. Uses OAuth 2.0 Client Credentials; optional
x_api_key for the Physical Foreign Flow (transmission) endpoint.
"""

import logging
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.terna.modules.fees import (
    DailyPricesReader,
)
from databricks.labs.community_connector.sources.terna.modules.load import (
    MarketLoadReader,
    TotalLoadReader,
    PeakValleyLoadReader,
    PeakValleyLoadDetailsReader,
    MonthlyIndexIndustrialElectricalConsumptionReader,
    DemandCoverageBySourceReader,
    ElectricalEnergyInItalyReader,
    ElectricalEnergyByTypeReader,
    ElectricalEnergyBySectorReader,
    IndustrySectorReader,
    MarketStatReader,
    TotalStatReader,
    ServicesSectorReader,
)
from databricks.labs.community_connector.sources.terna.utils import TernaApiClient

# =============================================================================
# Supported tables (static list)
# =============================================================================

SUPPORTED_TABLES = [
    TotalLoadReader.TOTAL_LOAD_KEY,
    MarketLoadReader.MARKET_LOAD_KEY,
    DailyPricesReader.DAILY_PRICES_KEY,
    PeakValleyLoadReader.PEAK_VALLEY_KEY,
    PeakValleyLoadDetailsReader.PEAK_VALLEY_LOAD_DETAILS_KEY,
    MonthlyIndexIndustrialElectricalConsumptionReader.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_KEY,
    DemandCoverageBySourceReader.DEMAND_COVERAGE_BY_SOURCE_KEY,
    ElectricalEnergyInItalyReader.ELECTRICAL_ENERGY_IN_ITALY_KEY,
    ElectricalEnergyByTypeReader.ELECTRICAL_ENERGY_BY_TYPE_KEY,
    ElectricalEnergyBySectorReader.ELECTRICAL_ENERGY_BY_SECTOR_KEY,
    IndustrySectorReader.INDUSTRY_SECTOR_KEY,
    MarketStatReader.MARKET_STAT_KEY,
    TotalStatReader.TOTAL_STAT_KEY,
    ServicesSectorReader.SERVICES_SECTOR_KEY,
]

# =============================================================================
# Table schemas
# =============================================================================

TABLE_SCHEMAS = {
    "total_load": TotalLoadReader.TOTAL_LOAD_SCHEMA,
    "market_load": MarketLoadReader.MARKET_LOAD_SCHEMA,
    "daily_prices": DailyPricesReader.DAILY_PRICES_SCHEMA,
    "peak_valley_load": PeakValleyLoadReader.PEAK_VALLEY_LOAD_SCHEMA,
    "peak_valley_load_details": PeakValleyLoadDetailsReader.PEAK_VALLEY_LOAD_DETAILS_SCHEMA,
    "monthly_index_industrial_electrical_consumption": MonthlyIndexIndustrialElectricalConsumptionReader.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SCHEMA,
    "demand_coverage_by_source": DemandCoverageBySourceReader.DEMAND_COVERAGE_BY_SOURCE_SCHEMA,
    "electrical_energy_in_italy": ElectricalEnergyInItalyReader.ELECTRICAL_ENERGY_IN_ITALY_SCHEMA,
    "electrical_energy_by_type": ElectricalEnergyByTypeReader.ELECTRICAL_ENERGY_BY_TYPE_SCHEMA,
    "electrical_energy_by_sector": ElectricalEnergyBySectorReader.ELECTRICAL_ENERGY_BY_SECTOR_SCHEMA,
    "industry_sector": IndustrySectorReader.INDUSTRY_SECTOR_SCHEMA,
    "market_stat": MarketStatReader.MARKET_STAT_SCHEMA,
    "total_stat": TotalStatReader.TOTAL_STAT_SCHEMA,
    "services_sector": ServicesSectorReader.SERVICES_SECTOR_SCHEMA,
}

# =============================================================================
# Table metadata: primary keys, cursor field, ingestion type (all append)
# =============================================================================

TABLE_METADATA = {
    "total_load": TotalLoadReader.TOTAL_LOAD_METADATA,
    "market_load": MarketLoadReader.MARKET_LOAD_METADATA,
    "daily_prices": DailyPricesReader.DAILY_PRICES_METADATA,
    "peak_valley_load": PeakValleyLoadReader.PEAK_VALLEY_LOAD_METADATA,
    "peak_valley_load_details": PeakValleyLoadDetailsReader.PEAK_VALLEY_LOAD_DETAILS_METADATA,
    "monthly_index_industrial_electrical_consumption": MonthlyIndexIndustrialElectricalConsumptionReader.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_METADATA,
    "demand_coverage_by_source": DemandCoverageBySourceReader.DEMAND_COVERAGE_BY_SOURCE_METADATA,
    "electrical_energy_in_italy": ElectricalEnergyInItalyReader.ELECTRICAL_ENERGY_IN_ITALY_METADATA,
    "electrical_energy_by_type": ElectricalEnergyByTypeReader.ELECTRICAL_ENERGY_BY_TYPE_METADATA,
    "electrical_energy_by_sector": ElectricalEnergyBySectorReader.ELECTRICAL_ENERGY_BY_SECTOR_METADATA,
    "industry_sector": IndustrySectorReader.INDUSTRY_SECTOR_METADATA,
    "market_stat": MarketStatReader.MARKET_STAT_METADATA,
    "total_stat": TotalStatReader.TOTAL_STAT_METADATA,
    "services_sector": ServicesSectorReader.SERVICES_SECTOR_METADATA,
}

class TernaLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Terna Public API."""

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Terna connector.

        Expected options:
            - client_id: OAuth 2.0 Client Credentials application key (required).
            - client_secret: OAuth 2.0 Client Credentials secret (required).
            - base_url: Base URL for the API (default https://api.terna.it).
            - x_api_key: Optional API key for Physical Foreign Flow when that
              endpoint uses x-api-key instead of Bearer token.
        """
        super().__init__(options)
        self._client = TernaApiClient(options, SUPPORTED_TABLES)
        self._total_load_reader = TotalLoadReader(self._client)
        self._market_load_reader = MarketLoadReader(self._client)
        self._daily_prices_reader = DailyPricesReader(self._client)
        self._peak_valley_load_reader = PeakValleyLoadReader(self._client)
        self._peak_valley_load_details_reader = PeakValleyLoadDetailsReader(self._client)
        self._monthly_index_industrial_electrical_consumption_reader = MonthlyIndexIndustrialElectricalConsumptionReader(self._client)
        self._demand_coverage_by_source_reader = DemandCoverageBySourceReader(self._client)
        self._electrical_energy_in_italy_reader = ElectricalEnergyInItalyReader(self._client)
        self._electrical_energy_by_type_reader = ElectricalEnergyByTypeReader(self._client)
        self._electrical_energy_by_sector_reader = ElectricalEnergyBySectorReader(self._client)
        self._industry_sector_reader = IndustrySectorReader(self._client)
        self._market_stat_reader = MarketStatReader(self._client)
        self._total_stat_reader = TotalStatReader(self._client)
        self._services_sector_reader = ServicesSectorReader(self._client)

    def list_tables(self) -> list[str]:
        """List names of all tables supported by this connector."""
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Return the Spark schema for the given table."""
        self._client.validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Return metadata (primary_keys, cursor_field, ingestion_type) for the table."""
        self._client.validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read records by date-range chunks; cursor is the last 'date' value (yyyy-mm-dd hh:mm:ss)."""
        self._client.validate_table(table_name)
        reader = {
            "total_load": self._total_load_reader.read,
            "market_load": self._market_load_reader.read,
            "daily_prices": self._daily_prices_reader.read,
            "peak_valley_load": self._peak_valley_load_reader.read,
            "peak_valley_load_details": self._peak_valley_load_details_reader.read,
            "monthly_index_industrial_electrical_consumption": self._monthly_index_industrial_electrical_consumption_reader.read,
            "demand_coverage_by_source": self._demand_coverage_by_source_reader.read,
            "electrical_energy_in_italy": self._electrical_energy_in_italy_reader.read,
            "electrical_energy_by_type": self._electrical_energy_by_type_reader.read,
            "electrical_energy_by_sector": self._electrical_energy_by_sector_reader.read,
            "industry_sector": self._industry_sector_reader.read,
            "market_stat": self._market_stat_reader.read,
            "total_stat": self._total_stat_reader.read,
            "services_sector": self._services_sector_reader.read,
        }[table_name]
        return reader(start_offset, table_options)
