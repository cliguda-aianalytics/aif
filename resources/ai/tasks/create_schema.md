# Creating a New Schema

## Required Inputs
1. **PRJ_NAME** (project name) - usually "ai-analytics"
2. **DWH_NAME** (data warehouse name) - e.g., DWH_USE_CASES, DWH_FINANCE, DWH_AZZEED
3. **SCHEMA_NAME** (schema name) - e.g., raw_kaggle, core_planning (lowercase with underscores)

## Optional Inputs
- **COMMENT_FOR_SCHEMA** - Description of the schema's purpose

## Step-by-Step Process

### Step 1: Gather Inputs
- Collect all required inputs from the user's request
- If any required input is missing, ask the user to provide it
- NEVER make assumptions without explicit user confirmation

### Step 2: Initial Confirmation
**⚠️ STOP HERE - ASK FOR CONFIRMATION**
- Present the collected inputs to the user
- Ask: "I'll create a new schema named <SCHEMA_NAME> in database <DWH_NAME> for project <PRJ_NAME>. Is this correct?"
- Wait for user confirmation before proceeding

### Step 3: Create Schema Package Structure
- Create new Python package directory: `aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>/`
- This should include:
  - `__init__.py` file
  - `wf/` directory with `__init__.py`
  - Any other necessary subdirectories based on requirements

### Step 4: Create Schema __init__.py
- Copy template: `aif/_template/dwh_schema_init.py`
- Save as: `aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>/__init__.py`
- Replace placeholders:
  - Set `DWH_NAME = "<DWH_NAME>"` (UPPERCASE)
  - Set `SCHEMA_NAME = "<SCHEMA_NAME>"` (UPPERCASE)

### Step 5: Update Project Definitions
**Update TWO definition files:**

1. **Database-level definitions**: `aif/<PRJ_NAME>/<DWH_NAME>/definitions.py`
   - Import the schema definition
   - Add to the definitions list

2. **Project-level definitions**: `aif/<PRJ_NAME>/definitions.py`
   - Import the schema definition
   - Add to the definitions list

### Step 6: Create Schema Asset File
- Copy template: `aif/_template/asset_templates/asset_schema.p_`
- Save as: `aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>/wf/asset_schema.py`
- Replace all placeholders:
  - `<PRJ_NAME>` → project name without "ai-" prefix (e.g., "ai_analytics" for "ai-analytics")
  - `<DWH_NAME>` → database name (UPPERCASE)
  - `<SCHEMA_NAME>` → schema name (UPPERCASE)
  - `<COMMENT>` → schema description (use COMMENT_FOR_SCHEMA or generate descriptive text)

### Step 7: Create wf/__init__.py
- Create file: `aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>/wf/__init__.py`
- Content:
```python
"""Workflow module for <schema_name> schema assets."""

from aif.<PRJ_NAME>.<DWH_NAME>.<SCHEMA_NAME>.wf.asset_schema import asset_schema

__all__ = ["asset_schema"]
```

### Step 8: Verify Structure
Check that the following structure exists:
```
aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>/
├── __init__.py             # With DWH_NAME and SCHEMA_NAME constants
└── wf/
    ├── __init__.py         # With imports and __all__
    └── asset_schema.py     # Schema creation asset
```

### Step 9: Materialize Schema
- Run: `uv run dagster asset materialize -f aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>/wf/asset_schema.py --select 'key:"<DWH_NAME>/<SCHEMA_NAME>/SCHEMA"'`
- Verify successful creation

## Important Notes

### Naming Conventions
- **DWH_NAME**: Always UPPERCASE (e.g., DWH_USE_CASES)
- **SCHEMA_NAME**: Always UPPERCASE in constants, lowercase in directory names
- **Directory names**: Use lowercase with underscores (e.g., raw_kaggle)
- **Asset names**: Use "SCHEMA" for schema creation assets

### Common Pitfalls to Avoid
1. **Missing Confirmations**: Always stop and ask for user confirmation
2. **Wrong Case**: DWH_NAME and SCHEMA_NAME constants must be UPPERCASE
3. **Missing Imports**: Remember to update BOTH definition files
4. **Incomplete Structure**: Create all necessary directories and __init__.py files
5. **Template Placeholders**: Replace ALL placeholders, including brackets

### Template Placeholders Reference
- `<PRJ_NAME>` → Project name for imports (e.g., "ai_analytics")
- `<DWH_NAME>` → Database name constant
- `<SCHEMA_NAME>` → Schema name constant
- `<COMMENT>` → Schema description

## Examples
If creating schema `RAW_KAGGLE` in database `DWH_USE_CASES`:
- Directory: `aif/ai_analytics/dwh_use_cases/raw_kaggle/`
- Constants: `DWH_NAME = "DWH_USE_CASES"`, `SCHEMA_NAME = "RAW_KAGGLE"`
- Import path: `from aif.ai_analytics.dwh_use_cases.raw_kaggle import ...`