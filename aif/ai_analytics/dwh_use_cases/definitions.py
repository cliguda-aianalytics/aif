"""Main entry point for the DWH Use Cases database in Dagster.

This module serves as the central entry point for the DWH Use Cases database and should be
referenced in workspace.yaml to be visible in Dagster. It combines the definitions from
multiple schema layers into a unified Dagster Definitions object, enabling a cohesive view
of all assets, schedules, and other components across the database.

The module imports schema definitions from each layer:

"""

import dagster as dg

from aif.ai_analytics.dwh_use_cases.placeholder_schema import SCHEMA_DEFINITION as PLACEHOLDER_SCHEMA_DEFINITION
from aif.common.dagster.util import DagsterSchemaDefinitions, create_main_defs

DEFINITIONS: list[DagsterSchemaDefinitions] = [
    PLACEHOLDER_SCHEMA_DEFINITION,
]

global_defs: dg.Definitions = create_main_defs(definitions=DEFINITIONS)
