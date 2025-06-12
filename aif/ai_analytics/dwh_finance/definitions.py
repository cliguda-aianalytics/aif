"""Main entry point for the DWH Finance database in Dagster.

This module serves as the central entry point for the DWH Finance database and should be
referenced in workspace.yaml to be visible in Dagster. It combines the definitions from
multiple schema layers (RAW_FIN_DATA_YF and CORE_FIN_DATA_YF) into a unified Dagster
Definitions object, enabling a cohesive view of all assets, schedules, and other
components across the database.

The module imports schema definitions from each layer and merges them into a single
Definitions object that Dagster can use to orchestrate the entire data pipeline.
"""

import dagster as dg

from aif.common.dagster.util import DagsterSchemaDefinitions, create_main_defs
from aif.ai_analytics.dwh_finance.core_fin_data_yf import SCHEMA_DEFINITION as DWH_CORE_FIN_DATA_YF_DEFS
from aif.ai_analytics.dwh_finance.raw_fin_data_yf import SCHEMA_DEFINITION as DWH_RAW_FIN_DATA_YF_DEFS

DEFINITIONS: list[DagsterSchemaDefinitions] = [
    DWH_RAW_FIN_DATA_YF_DEFS,
    DWH_CORE_FIN_DATA_YF_DEFS,
]

global_defs: dg.Definitions = create_main_defs(definitions=DEFINITIONS)
