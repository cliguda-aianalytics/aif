# Creating a new Database Object

## Required Inputs
1. **PRJ_NAME** (project name) - usually "ai-analytics"
2. **DWH_NAME** (data warehouse name) - e.g., DWH_USE_CASES, DWH_FINANCE, etc.
3. **SCHEMA_NAME** (schema name) - e.g., raw_planning_data, core_planning
4. **ASSET_NAME** (Name of the table, view, function) - UPPERCASE
5. **ASSET_SPECIFICATION** (SQL-DDL code or a description of the asset)

## Optional Inputs
- **COMMENT_FOR_DB_OBJ** - Description for the database object

## Step-by-Step Process

### Step 1: Gather Inputs
- Collect all required inputs from the user's request
- If any required input is missing, ask the user to provide it

### Step 2: Initial Confirmation
**⚠️ STOP HERE - ASK FOR CONFIRMATION**
- Present the collected inputs to the user
- Ask: "I'll create a <TABLE/VIEW/FUNCTION> named <ASSET_NAME> in schema <SCHEMA_NAME>. Is this correct?"
- Wait for user confirmation before proceeding

### Step 3: Check Schema Existence
- Check if schema exists in path: `aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>`
- If schema doesn't exist, follow the create_schema.md task first

### Step 4: Determine Database Type
**IMPORTANT**: Check the database type to use correct SQL syntax
- Read the config file: `aif/<PRJ_NAME>/resources/config/dev/dwh.yaml`
- Look for the DWH_NAME entry to find the "type" field (postgres/snowflake)
- Note: Snowflake uses UPPERCASE for object names, PostgreSQL uses lowercase

### Step 5: Generate DDL Statement
Based on ASSET_SPECIFICATION:
- If DDL provided: use it, but adapt to database type syntax
- If description provided: generate DDL with these rules:
  - **Snowflake**: Use UPPERCASE for all object names, VARCHAR(500) for strings
  - **PostgreSQL**: Use lowercase for all object names, VARCHAR(500) for strings
  - Include appropriate data types and constraints
  - Add PRIMARY KEY where applicable
  - **TABLE Creation**: Use `CREATE TABLE IF NOT EXISTS` for Snowflake and `CREATE TABLE IF NOT EXISTS` for PostgreSQL
  - **Other Objects**: Use `CREATE OR REPLACE` for views, functions, and other database objects

### Step 6: DDL Confirmation
**⚠️ STOP HERE - ASK FOR CONFIRMATION**
- Present the generated DDL statement to the user
- Ask: "Here's the DDL statement I'll use. Is this correct?"
- Wait for user confirmation before proceeding

### Step 7: Create SQL File
- Create directory if needed: `<schema_path>/resources/sql/ddl/`
- Create file: `<schema_path>/resources/sql/ddl/<asset_name_lowercase>.sql`
- File content should NOT use template variables for schema names in Snowflake
- Use direct schema references: `SCHEMA_NAME.TABLE_NAME` (not `{{ SCHEMA_NAME }}.TABLE_NAME`)
- **IMPORTANT**: Always use `{{ COMMENT }}` placeholder for database object comments to ensure consistency:
  - Add `COMMENT = '{{ COMMENT }}'` for tables/views
  - This ensures the comment matches between Dagster asset description and database object

### Step 8: Create Asset File
- Copy template: `aif/_template/asset_templates/asset_db_object.p_`
- Save as: `<schema_path>/wf/asset_<asset_name_lowercase>.py`
- Replace placeholders:
  - `<PRJ_NAME>` → project name (e.g., "ai_analytics")
  - `<DWH_NAME>` → database name
  - `<SCHEMA_NAME>` → schema name
  - `<ASSET_NAME>` → asset name (UPPERCASE)
  - `<ASSET_TYPE>` → type of database object (table, view, function, etc.)
  - `<COMMENT>` → description
  - `<DDL_FILENAME>` → full path from project root
  - `<asset_name_lowercase>` → asset name in lowercase for function names
  - Update function name to `asset_<asset_name_lowercase>`

### Step 9: Update Dependencies
**IMPORTANT**: Use the exact dependency format from template:
```python
deps=[dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "SCHEMA"]))]
```
Do NOT use direct asset references like `deps=[asset_schema]`

### Step 10: Update __init__.py
- Add import to `<schema_path>/wf/__init__.py`
- Add asset function to `__all__` list

### Step 11: Materialize and Verify
- Run: `uv run dagster asset materialize -f <asset_file_path> --select 'key:"<DWH_NAME>/<SCHEMA_NAME>/<ASSET_NAME>"'`
- Verify successful creation

## Important Notes

### Common Pitfalls to Avoid
1. **Missing Confirmations**: Always stop at confirmation points
2. **Wrong SQL Syntax**: Check database type before writing DDL
3. **Wrong Parameter Names**: Use `filename` (not `file_path`) for execute_statement_from_file
4. **Modified Dependencies**: Use exact template format for deps parameter
5. **Case Sensitivity**: Snowflake uses UPPERCASE, PostgreSQL uses lowercase

### Template Placeholders Reference
- `<PRJ_NAME>` → Project name for imports (e.g., "ai_analytics")
- `<DWH_NAME>` → Database name constant (UPPERCASE)
- `<SCHEMA_NAME>` → Schema name constant (UPPERCASE)
- `<ASSET_NAME>` → Database object name (UPPERCASE)
- `<ASSET_TYPE>` → Type of database object (table, view, function, etc.)
- `<COMMENT>` → Description for the database object
- `<DDL_FILENAME>` → Full path to DDL SQL file from project root
- `<asset_name_lowercase>` → Asset name in lowercase for function names