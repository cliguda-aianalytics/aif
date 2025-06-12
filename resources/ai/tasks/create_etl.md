# Creating a Simple ETL Pipeline

## Required Inputs
1. **PRJ_NAME** (project name) - usually "ai-analytics"
2. **DWH_NAME** (data warehouse name) - e.g., DWH_USE_CASES, DWH_FINANCE, etc.
3. **SCHEMA_NAME** (schema name) - e.g., raw_kaggle, core_planning
4. **ASSET_NAME** (Name of the target table) - UPPERCASE
5. **DATA_SOURCE** (Where to get the data from) - e.g., CSV file path, S3 bucket/key, API endpoint
6. **ETL_DESCRIPTION** - Description of what the ETL pipeline does

## Optional Inputs
- **MINIMAL_TRANSFORMS** - Only essential transformations (data type conversions, JSON flattening)
- **SCHEDULE** - Cron schedule for automatic execution (e.g., "0 1 * * *" for daily at 1 AM)

## Step-by-Step Process

### Step 1: Gather Inputs
- Collect all required inputs from the user's request
- If any required input is missing, ask the user to provide it

### Step 2: Initial Confirmation
**⚠️ STOP HERE - ASK FOR CONFIRMATION**
- Present the collected inputs to the user
- Ask: "I'll create an ETL pipeline for table <ASSET_NAME> in schema <SCHEMA_NAME> that loads data from <DATA_SOURCE>. Is this correct?"
- Wait for user confirmation before proceeding

### Step 3: Check Prerequisites
- Verify schema exists in path: `aif/<PRJ_NAME>/<DWH_NAME>/<SCHEMA_NAME>`
- If schema doesn't exist, follow the create_schema.md task first
- Verify target table exists in: `<schema_path>/wf/asset_<asset_name_lowercase>.py`
- If table doesn't exist, follow the create_db_obj.md task first

### Step 4: Determine Database Type
**IMPORTANT**: Check the database type to use correct SQL syntax
- Read the config file: `aif/<PRJ_NAME>/resources/config/dev/dwh.yaml`
- Look for the DWH_NAME entry to find the "type" field (postgres/snowflake)
- Note: This affects database configuration in the ETL class

### Step 5: Create ETL Source Class
- Copy template: `aif/_template/src_templates/table_etl.p_`
- Save as: `<schema_path>/src/<asset_name_lowercase>_etl.py`
- Replace placeholders:
  - `<ASSET_NAME>` → table name (PascalCase for class name)
  - `<SCHEMA_NAME>` → schema name (lowercase string)
  - `<EXTRACT>` → extraction logic based on DATA_SOURCE type
  - `<TRANSFORM>` → minimal transformation logic (only essential conversions)
  - `<DML_FILENAME>` → path to DML file (if using insert from file)

### Step 6: Implement Data Source Logic
Based on DATA_SOURCE type, implement appropriate extraction logic.
See the **Common Data Source Patterns** section below for specific implementation examples for:
- CSV files (local storage)
- CSV files from S3
- JSON files  
- API endpoints

Apply only minimal transformations as outlined in the **Minimal Transformation Philosophy** section.

### Step 7: Create DML File (Optional)
- If using complex insert logic, create: `<schema_path>/resources/sql/dml/<asset_name_lowercase>_insert.sql`
- Use parameterized SQL with placeholders for pandas DataFrame columns

### Step 8: ETL Implementation Confirmation
**⚠️ STOP HERE - ASK FOR CONFIRMATION**
- Present the ETL class implementation to the user
- Ask: "Here's the ETL implementation I'll create. Does this look correct?"
- Wait for user confirmation before proceeding

### Step 9: Create ETL Asset
- Copy template: `aif/_template/asset_templates/asset_table_etl.p_`
- Save as: `<schema_path>/wf/asset_<asset_name_lowercase>_etl.py` (create new file)
- OR add ETL asset to existing: `<schema_path>/wf/asset_<asset_name_lowercase>.py`
- Replace placeholders:
  - `<PRJ_NAME>` → project name (e.g., "ai_analytics")
  - `<DWH_NAME>` → database name
  - `<SCHEMA_NAME>` → schema name
  - `<ASSET_NAME>` → table name (UPPERCASE)
  - `<ETL_DESCRIPTION>` → description
  - `<ETL_CLASS_NAME>` → ETL class name (PascalCase)
  - Function name: `asset_<asset_name_lowercase>_etl`

### Step 10: Configure ETL Asset Dependencies
**IMPORTANT**: ETL asset depends on the table creation asset:
```python
deps=[dg.SourceAsset(key=dg.AssetKey([DWH_NAME, SCHEMA_NAME, "<ASSET_NAME>"]))]
```

### Step 11: Add Scheduling (Optional)
If SCHEDULE provided, add auto-materialize policy:
```python
auto_materialize_policy=dg.AutoMaterializePolicy.eager().with_rules(
    dg.AutoMaterializeRule.materialize_on_cron("<SCHEDULE>", timezone="UTC"),
),
```

### Step 12: Update Configuration Files
Add any additional config files needed:
- For S3 access: add S3 config file to CONFIG_FILES
- For API access: add API config file to CONFIG_FILES

### Step 13: Update __init__.py
- Add import to `<schema_path>/wf/__init__.py`
- Add ETL asset function to `__all__` list
- Add ETL source class import to `<schema_path>/src/__init__.py`

### Step 14: Materialize and Verify
- First materialize table: `uv run dagster asset materialize -f <table_asset_file> --select 'key:"<DWH_NAME>/<SCHEMA_NAME>/<ASSET_NAME>"'`
- Then materialize ETL: `uv run dagster asset materialize -f <etl_asset_file> --select 'key:"<DWH_NAME>/<SCHEMA_NAME>/<asset_name_lowercase>_etl"'`
- Verify data was loaded successfully

## Important Notes

### Template Files to Create

#### ETL Asset Template
Create: `aif/_template/asset_templates/asset_table_etl.p_`

#### ETL Source Class Template  
Update existing: `aif/_template/src_templates/table_etl.p_`

### Common Data Source Patterns

#### CSV File from Local Storage
```python
df = pd.read_csv("<CSV_PATH>", sep=",", encoding="utf-8", header=0)
```

#### CSV File from S3
```python
from aif.common.aif.src.data_interfaces.s3 import download_file_to_memory
from io import BytesIO

file_bytedata = download_file_to_memory(bucket_name="<BUCKET>", object_key="<KEY>")
df = pd.read_csv(BytesIO(file_bytedata), sep=",", encoding="utf-8", header=0)
```

#### JSON File from Local Storage
```python
df = pd.read_json("<JSON_PATH>", orient="records")
```

#### API Data
```python
import requests
response = requests.get("<API_URL>")
df = pd.DataFrame(response.json())
```

### Minimal Transformation Philosophy

**IMPORTANT**: ETL transformations should be kept to a minimum. Most data processing should happen in the DWH using SQL in subsequent layers (PRE, INT, CORE).

### Avoid These Transformations (Do in DWH Instead)
- Data cleaning (nulls, duplicates, whitespace) → Do in PRE layer
- Data standardization and normalization → Do in PRE layer  
- Business logic and calculations → Do in INT layer
- Final business-ready entity creation → Do in CORE layer
- Aggregations and summarizations → Do in APP layer

### Essential Transformations Only
Only perform transformations that are **absolutely necessary** to load data into the database:

#### 1. Data Type Conversions (Essential)
```python
# Only convert types that are required for database loading
df["date_column"] = pd.to_datetime(df["date_column"])  # If dates aren't recognized
df["numeric_column"] = pd.to_numeric(df["numeric_column"], errors='coerce')  # If needed for DB schema
```

#### 2. JSON/Nested Data Flattening (Essential)
```python
# Only flatten if required to fit table schema
df = pd.json_normalize(df["nested_column"])  # If loading nested JSON
```

#### 3. Critical Data Issues (Essential)
```python
# Only fix issues that would prevent loading
df.columns = df.columns.str.replace(' ', '_')  # Fix column names for DB compatibility

# For Snowflake databases: Convert column names to uppercase
df.columns = df.columns.str.upper()  # Required for Snowflake compatibility
```



### Common Pitfalls to Avoid
1. **Missing Confirmations**: Always stop at confirmation points
2. **Wrong Dependencies**: ETL asset must depend on table creation asset
3. **Missing Imports**: Import ETL class in asset file
4. **Wrong DB Config**: Use correct database configuration name
5. **File Path Issues**: Use correct paths for data sources and templates
6. **Missing Config Files**: Include all necessary config files in CONFIG_FILES
7. **Schedule Format**: Use correct cron format for scheduling
8. **Error Handling**: Implement proper error handling in extract/transform/load methods
9. **Over-transformation**: Avoid doing data processing in ETL - keep transformations minimal
10. **Business Logic in ETL**: Don't implement business rules in ETL - do them in INT layer of DWH

### Template Placeholders Reference
- `<PRJ_NAME>` → Project name for imports (e.g., "ai_analytics")
- `<DWH_NAME>` → Database name constant (UPPERCASE)
- `<SCHEMA_NAME>` → Schema name constant (UPPERCASE)
- `<ASSET_NAME>` → Table name (UPPERCASE)
- `<DATA_SOURCE>` → Description of data source location
- `<ETL_DESCRIPTION>` → Description of ETL pipeline purpose
- `<ETL_CLASS_NAME>` → ETL class name (PascalCase)
- `<asset_name_lowercase>` → Asset name in lowercase for function names
- `<DML_FILENAME>` → Path to DML SQL file (if using custom insert logic)
- `<SCHEDULE>` → Cron schedule string (e.g., "0 1 * * *")
- `<CSV_PATH>`, `<BUCKET>`, `<KEY>`, `<JSON_PATH>`, `<API_URL>` → Data source specific paths/parameters