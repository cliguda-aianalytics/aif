# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Tasks

## ⚠️ CRITICAL: Task Identification
**BEFORE starting ANY work, ALWAYS check if the user's request matches a predefined task:**

**FIRST ACTION**: Check if user request matches any predefined task below
- If YES: Stop everything and follow the task file EXACTLY
- If NO: Continue with normal workflow

## Execution Rules
- **MANDATORY**: Follow task steps in EXACT order
- **MANDATORY**: Stop at all confirmation points marked with ⚠️
- **MANDATORY**: Ask for user confirmation before proceeding
- When input is required, make reasonable assumptions based on context, but ALWAYS ask for verification before continue!
- When using templates, ALWAYS use them exactly as provided without modifications unless explicitly instructed.
- **CRITICAL Path Handling**: All paths and filenames in task files are relative to the project root directory. Replace placeholders (e.g., `<PRJ_NAME>`, `<DWH_NAME>`) with actual values, but do not shorten, extend, or otherwise modify the path structure.

## Available Tasks

### Create a new schema
- Use: `resources/ai/tasks/create_schema.md`
- Task Matching Examples: "Create a schema", "Add a new schema"

### Create a database object (table, view, function)
- Use: `resources/ai/tasks/create_db_obj.md`
- Task Matching Examples: "Create a table named ...", "Create a view for ...", "Build a function that ..."

### Load data into a table
- Use: `resources/ai/tasks/create_etl.md`
- Task Matching Examples: "Load data from CSV ...", "Load data from file ...", "Create a ETL for file ..."

---

# AIF
AIF (Analytical Intelligence Framework) is a framework to build a Data Platform with one or multiple DWHs, 
together with data analytics, ML and AI applications.

## Tech-Stack
- Python 3.12 and uv for dependencies
- Postgres can be used as DWH
- Dagster is used for workflow management, automation and data quality checks.
- Use uv to add libraries (uv add ...) and to run scripts or commands, e.g. uv run python -m <package>.<module>

# Best Practices

## Python
- Target CPython 3.12 and type-hint everything (classes, *args, **kwargs, pd.DataFrame, np.ndarray, …).
- Import order: std-lib > third-party > aif internal. Blank line between the three blocks. No wildcard imports.
- Always prefer absolute package imports, never relative dot-dot imports.
- Use f-strings everywhere except inside logging.* calls – those must use lazy % formatting.
- Prefer dataclasses (@dataclass) for plain configuration containers + `__post_init__` if validation logic is needed.
- Avoid trailing whitespace (trailing-whitespace) in empty lines
- Always check and respect the return types of methods and functions
- Use proper type hints for all variables, especially when handling return values
- Default line length: 120 chars

## Testing Best Practices
- **IMPORTANT**: When fixing type errors in tests, prefer changing the test to use valid inputs rather than modifying the tested method signatures - maintain interface consistency by fixing tests, not changing method signatures
- Tests should use realistic test data that matches the expected types and contracts

## Naming Conventions
- Constants in UPPER_CASE, module private helpers start with a single leading underscore.
- Variables / functions: snake_case, Classes: PascalCase, Modules / packages: snake_case.
- Variable suffixes: Pandas DataFrame: _df, Pandas Series: _s, Numpy array: _npa, Numpy scalar: _nps, Snowflake DataFrame: _sdf

## Error Handling
- Keep try: blocks minimal (1-2 lines).
- Catch the error, log it, then raise so the caller decides.
- Inside loops continue on recoverable errors but log WARNING with the offending key / row id.

## Docstrings & Comments
- Every module begins with a high-level docstring.
- Function docstrings must list Args / Returns; raise sections only if raised in the function.
- Comment on code only when not self-explanatory.

# Architecture Overview

## Project Structure
The main project folder <PRJ_NAME> is "ai-analytics" and ALWAYS use "ai-analytics" when PRJ_NAME is required.
```
aif/
├──  <PRJ_NAME>/         # domain-specific code (grouped by DWH / schema)
├──  common/        # code that is reused across domains
├──  _template/     # cookie-cutter blueprints; never imported at runtime
├──  resources/     # Resources for deployment
├──  scripts/       # Helping scripts for the CICD pipeline
```

## Layered DWH Architecture

The schemas in the DWH are structured as follows:

- **STG Layer**: Staging area for external data tools (Fivetran, Airbyte) - unstructured cloud storage (prefix stg_*)
- **RAW Layer**: Initial data ingestion from staging with minimal transformations (prefix raw_*)
- **PRE Layer**: Data cleaning and pre-processing of single tables (prefix pre_*)
- **INT Layer**: Integration layer to merge data sources + business logic application (prefix int_*)
- **CORE Layer**: Business-ready final tables for consumption (prefix core_*)
- **APP Layer**: Application-specific aggregations and views (prefix app_*)

## Data Flow Architecture
```
External Sources → STG (cloud storage/stages) → RAW (structured tables) → PRE → INT → CORE → APP
```

## Schema Structure Pattern

- Each schema follows this standardized structure:
```
schema_name/
├── __init__.py             # Schema definition with SCHEMA_DEFINITION
├── api/                    # For api endpoints
├── notebooks/              # Jupyter Notebooks
├── resources/              # Non Python files with sub-folders
│   ├── config/             # YAML files (see CONFIGURATION MANAGEMENT)
│   ├── sql/ddl/            # Data definition SQL
│   ├── sql/dml/            # Data manipulation SQL  
│   ├── sql/dql/            # Data querying SQL
│   ├── prompts/            # Prompts for AI applications
│   ├── ml_models/          # Local saved models (Only for first prototypes)
├── scripts/                # stand-alone entrypoints (mostly used while developing, not in production)
├── src/                    # business logic and ETL classes
├── test/                   # Tests (Subfolders unit_test, e2e_tests, sql_tests)
├── wf/                     # All Dagster specific code
│   ├── asset_schema.py     # Schema creation asset
│   └── asset_*.py          # Table and ETL assets
```
- When creating or modifying a new schema/python package ONLY create the directories that are relevant for the task.
- When creating a new directory, except in resources/, always create an __init__.py file.
- Scripts should be tiny (only a main() guarded by `if __name__ == "__main__":`) and delegate all real work to src/ code.
- Multiple SQL statements in one sql file MUST be separated by a "-- AIF: NEW_STATEMENT --" between them.

# Development

## Configuration Management
- Scripts, workflows, Notebooks define a list of yaml configuration files named CONFIG_FILES.
- CONFIG_FILES must include "aif/common/aif/resources/config/base.yaml" plus additional YAML files.
- Scripts, workflows, Notebooks call `initialize_aif(config_files=CONFIG_FILES)` first to initialize everything. 
  (`from aif.common.aif.src.initialization import initialize_aif`)
- Do not store business secrets in the repo – reference them through `${ENV_VAR}` in YAML and load with `initialize_aif`.
- When configurations in different environments like prod and dev are very different, they can have different configuration files by using the {ENV} placeholder in the configuration filename like "resources/config/{ENV}/dwh.yaml"

## Logging
- Always import: `from aif.common.aif.src import aif_logging as logging`.
- Get logger: `logging.get_aif_logger(__name__)`.
- Allowed levels: DEBUG (deep details), INFO (state changes), WARNING (potential issues), ERROR (failures).
- Never f-string in logging; use % placeholders to defer string formatting.
- No print() in production code unless told otherwise

## Database
### Configuration and DB Access
- Default DB configs in "aif/ai_analytics/resources/config/{ENV}/dwh.yaml" (include in CONFIG_FILES)
- use `DBInterface()` (`from aif.common.aif.src.data_interfaces.db_interface import DBInterface`) for all DB access.
- Use DBInterface as context manager (`with DBInterface(db_cfg="dwh") as db:`),
  where "dwh" is the name of the DB config in the dwh.yaml.
- Complex SQL statements as files in resources/sql and called by a helper wrapper  `DBInterface`:
   `asset_call_db_method(…, method_name="create_schema" | "execute_statement_from_file" | …) `.
- When using `asset_call_db_method` with `method_name="execute_statement_from_file"`, use parameter `filename` (not `file_path` or `sql_file`)

### Database Design Guidelines
- **Views**: Default choice for data aggregation from existing small tables and simple statements 
- **Materialized Views**: For more complex queries with long execution times (Not always possible in Snowflake, depending on account type)
- **Tables**: For raw data and when partial updates are needed (especially for bigger tables). 

### SQL Syntax Guidelines
- **PostgreSQL**:
  - Use lowercase for all database object names
  - Schema references: `schema_name.table_name`
  - String types: VARCHAR(n) where n is specified, default to VARCHAR(500)

## Dagster

### Asset Patterns
- All database objects (schema, tables, views) and ETL pipelines are implemented as Dagster assets and are UPPERCASE (<NAME>)
- For Dagster assets creating a database object <ASSET_NAME> the asset function is named `asset_<asset_name_lowercase>`
- For Dagster assets modifying an existing database object <ASSET_NAME> the asset function is named `asset_<asset_name_lowercase>_<type>` : where <type> is etl, ai, refresh...

All assets follow these conventions:
- **Key prefix**: `[DWH_NAME, SCHEMA_NAME]`
- **Group name**: `f"{DWH_NAME}_{SCHEMA_NAME}"`
- **Dependencies**: Declared via `deps=[dg.SourceAsset(...)]`
- **Standard flow for Dependencies**: Schema → DB Objects like Table/VIEWS/FUNCTIONS → Updating like ETL, refresh, function calls

### Dagster / Data-Engineering Specifics
- Every schema exposes SCHEMA_DEFINITION: DagsterSchemaDefinitions in init.py 
- Each schema package exports two ALL-CAPS constants: DWH_NAME and SCHEMA_NAME which are the names where all assets, database objects etc of the package are located
- In Metadata only use MetadataValue
- Use `@dg.asset_check` to validate assets: `@dg.asset_check(asset=<asset_function>, name="check_<check_name>", description="<description>"`)
- The asset function for a `@dg.asset_check` is `<asset_function>_check_<check_name>`
- After creating an asset, materialize it to verify, that it is working.

## Notebooks
For every Jupyter Notebook the first 5 cells are:
```
- <Docstring>
- imports 
- CONFIG_FILES = [...] 
- load_dotenv()
- initialize_aif(config_files=CONFIG_FILES)
```

## Development Commands

**Environment Setup:**
```bash
# Install dependencies
uv sync

# Install with all groups (default)
uv sync --all-groups
```

**Code Quality:**
```bash
# Format code
uv run black --line-length 120 .

# Lint code  
uv run pylint aif/

# Type checking
uv run mypy aif/

# Format notebooks
uv run nbqa black aif/ 

# Lint YAML files
uv run yamllint .
```

**Testing:**
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest path/to/test_file.py

# Run with verbose logging (configured in pytest.ini)
uv run pytest -v
```

**Dagster Operations:**
<KEY_PREFIX> must be replaced with the key_prefix of the asset, whereby each part is separated by / (e.g. [A,B] -> A/B)
```bash
# Start Dagster UI locally
uv run dagster dev

# Materialize one asset
uv run dagster asset materialize -f <FILENAME_FROM_PROJECT_ROOT> --select 'key:"<KEY_PREFIX>/<ASSET_NAME>"'

# Materialize all assets (from file)
uv run dagster asset materialize (-f <FILENAME_FROM_PROJECT_ROOT>) --select "*"

# Materialize specific asset group
uv run dagster asset materialize --select "tag:group_name"

```

**Scripts:**
```bash
# Run module with uv: 
uv run python -m <package>.<module>
# Run script with uv: 
uv run python scripts/script_name.py
```
