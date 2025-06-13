# AIF
AIF (Analytical Intelligence Framework) is a framework to build a Data Platform with one or multiple DWHs, 
together with data analytics, ML and AI applications.

AIF is an AI-first project, leveraging to power of AI for building a Data-Platform. We provide knowledge and task 
descriptions optimized for Claude Code.

To see AIF in action, visit https://www.youtube.com/channel/UCxBXSrE6dmleUEv1cAXpbkg.

To get into the vibe, use Claude Code for going through the setup process, especially if a
command is not working (install routine was tested on Ubuntu 24, but AI can help to adjust it).

# Version
AIF is published as Source Available (see LICENSE.md). If you want to bring AIF into production, contact us to build your Data Platform.
Our professional version includes:
- Snowflake integration
- Configuration scripts for setting up users and databases
- CICD pipeline (linting, testing, building, versioning and deployment to the Dagster Cloud)
- Cloud integrations (AWS, Azure, GCP)
- Advanced ETL Pipelines for Dagster
- Constantly improving ai-tasks

# AI Knowledge and Tasks
To understand how we use AI to operate on our codebase, check CLAUDE.md for general knowledge
and the resources/ai/tasks folder for individual tasks.

# Requirements

## Note
NOTE: Avoid to use "aif" as main project folder, since the main folder of the project is also called "aif" and AIs have often problems with the "aif/aif" part in the path and shorten it to "aif" (sadly true...)
Otherwise rename CLAUDE.local.m_ to CLAUDE.local.md and adjust the project directory, this also seems to work, since it mention the full name directly to the AI.

## PostgreSQL

### Install Postgres
```bash
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install -y postgresql
```

### Configure the Local Database

```bash
# Change to postgres user for database setup
sudo su - postgres

# Start editing the database
psql

CREATE USER aif WITH PASSWORD 'xxx';
# If the password is changed here, adjust it in .env

CREATE DATABASE dwh_use_cases;
GRANT ALL PRIVILEGES ON DATABASE dwh_use_cases TO aif;

CREATE DATABASE dwh_finance;
GRANT ALL PRIVILEGES ON DATABASE dwh_finance TO aif;

CREATE USER aif_test WITH PASSWORD 'xxx';
# If the password is changed here, adjust it in aif/ai_analytics/resources/config/dev_test/dwh.yaml

CREATE DATABASE dwh_test;
GRANT ALL PRIVILEGES ON DATABASE dwh_test TO aif_test;
\q   # Quit psql

exit # Exit user postgres
```

### Restart Postgres

```bash
sudo systemctl restart postgresql
```

## pipx
pipx is recommended to install uv. Other installations can cause problems.
```bash
sudo apt update
sudo apt install pipx
pipx ensurepath
```

## uv
install uv via pipx, as snap installation has problems when used in AI coding assistants:
```bash
pipx install uv
```

# Setup

## Setting up virtual environment
```bash
uv venv
source .venv/bin/activate
uv sync
```

## Configuration
```bash
cp .env_template .env
```

## PATH configuration
Replace LOCAL_AIF_PATH with the root path of the project (The path, that contains the README.md)

NOTE: MAKE SURE, THE PATHS ENDS WITH A /
(e.g. /home/username/workspace/aif/)

## Postgres
Adjust password if needed.

## Check
Run pytest to verify, that modules are found and the postgres database is reachable.
```bash
uv run pytest
```

# Quickstart

## Dagster
- Run: 
```bash
dagster dev
```
- Browse to "Assets" - "View lineage" to understand to structure of the DWH
- Click "Materialize all" to create all database objects and load data from Yahoo Finance.
- Explore your Postgres DB to see all created database objects
- In Dagster click on "NB_OHLC_EXPLORE" - "VIEW NOTEBOOK" (Just a sample notebook to get the idea)

# Start vibing
Claude can make mistakes. If something is not working, Claude normally figures it out.

## Start
- Run:
```bash
claude
```

## Create a schema
- Start with "Create a schema raw_kaggle in dwh_use_cases"
- Explore your Postgres for the new schema

## Create your first table
- This project includes the Titanic dataset under resources/data/
- Copy the first 15-20 lines of resources/data/titanic_train.csv
- Prompt: 
  "Create a table kaggle_train under raw_kaggle that can store the following data:
  [INSERT DATA] 
  "
- Run:
```bash
dagster dev
```
- Browse to "Assets" - "View lineage" to see the added assets

## Load data into the table
- Prompt: Load data from resources/data/titanic_train.csv into raw_kaggle.kaggle_train 
- Explore the new table in Postgres

## Explore the code 
- The new code is here: ai_analytics/dwh_use_cases/raw_kaggle

## Learn more
- Ask Claude about the project...it's more accurate than any documentation.
- Subscribe to https://www.youtube.com/channel/UCxBXSrE6dmleUEv1cAXpbkg :-)
- Connect: https://www.linkedin.com/in/christian-liguda/
- Visit: https://ai-analytics.dev/