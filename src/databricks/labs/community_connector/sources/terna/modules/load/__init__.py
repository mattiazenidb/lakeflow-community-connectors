"""Load table readers."""

from databricks.labs.community_connector.sources.terna.modules.load.market_load_reader import (
    MarketLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.peak_valley_load_reader import (
    PeakValleyLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.peak_valley_load_details_reader import (
    PeakValleyLoadDetailsReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.total_load_reader import (
    TotalLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.monthly_index_industrial_electrical_consumption_reader import (
    MonthlyIndexIndustrialElectricalConsumptionReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.demand_coverage_by_source_reader import (
    DemandCoverageBySourceReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.electrical_energy_in_italy_reader import (
    ElectricalEnergyInItalyReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.electrical_energy_by_type_reader import (
    ElectricalEnergyByTypeReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.electrical_energy_by_sector_reader import (
    ElectricalEnergyBySectorReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.industry_sector_reader import (
    IndustrySectorReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.market_reader import (
    MarketStatReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.total_reader import (
    TotalStatReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.services_sector_reader import (
    ServicesSectorReader,
)

__all__ = [
    "MarketLoadReader",
    "TotalLoadReader",
    "PeakValleyLoadReader",
    "PeakValleyLoadDetailsReader",
    "MonthlyIndexIndustrialElectricalConsumptionReader",
    "DemandCoverageBySourceReader",
    "ElectricalEnergyInItalyReader",
    "ElectricalEnergyByTypeReader",
    "ElectricalEnergyBySectorReader",
    "IndustrySectorReader",
    "MarketStatReader",
    "TotalStatReader",
    "ServicesSectorReader",
]