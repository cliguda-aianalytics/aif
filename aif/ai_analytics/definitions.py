"""This is the main entry point for this project/code-location.
To use the whole project as one code location in Dagster, this file should be referenced in dagster_cloud.yaml
(e.g. as in resources/setup/prod_serverless/dagster_cloud.yaml)
Definitions from schemata are loaded, merged and loaded into a Dagster Definitions object."""

import dagster as dg

from aif.common.dagster.util import DagsterSchemaDefinitions, create_main_defs
from aif.ai_analytics.dwh_finance.core_fin_data_yf import SCHEMA_DEFINITION as DWH_CORE_FIN_DATA_YF_DEFS
from aif.ai_analytics.dwh_finance.raw_fin_data_yf import SCHEMA_DEFINITION as DWH_RAW_FIN_DATA_YF_DEFS
from aif.ai_analytics.dwh_use_cases.placeholder_schema import SCHEMA_DEFINITION as PLACEHOLDER_SCHEMA_DEFINITION

DEFINITIONS: list[DagsterSchemaDefinitions] = [
    DWH_RAW_FIN_DATA_YF_DEFS,
    DWH_CORE_FIN_DATA_YF_DEFS,
    PLACEHOLDER_SCHEMA_DEFINITION,
]

global_defs: dg.Definitions = create_main_defs(definitions=DEFINITIONS)
