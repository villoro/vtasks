# vtasks: Personal Pipeline
This repository contains my personal data pipeline, built with [Prefect](https://www.prefect.io/), to automate data extraction, transformation, and loading (ETL). The pipeline runs on my NAS and integrates multiple data sources, processing them and exporting the results to [MotherDuck](https://motherduck.com/) for analysis in [Metabase](https://www.metabase.com/).

## 📌 Pipeline Overview

The pipeline serves two main purposes:

1. **Automation:** Automates repetitive data tasks by extracting data from APIs, cloud services, and other sources.
2. **Analysis:** Loads transformed data into MotherDuck, where Metabase connects for visualization and insights.

## ⚙️ Prefect Workflow

The pipeline is orchestrated with Prefect, using flows and tasks to manage dependencies and execution. The scheduling logic is defined in `schedules/hourly.py`, ensuring that jobs run on an hourly basis. The execution happens on my NAS.

### Data Flow
1. **Extract:** Data is sourced from various integrations, including Dropbox, Google Sheets, APIs, and local files.
2. **Load:** Processed data is exported to MotherDuck for storage and analysis.
3. **Transform:** Data processing is primarily handled using `dbt`, with additional transformations as needed.
4. **Visualization:** Metabase connects to MotherDuck to generate insights and reports.

## 📂 Repository Structure

```plaintext
─ vtasks
 ├── common          # Shared utilities and helpers
 ├── jobs            # Individual data extraction and processing jobs
 │   ├── backups     # Backup management
 │   ├── crypto      # Crypto price tracking
 │   ├── dropbox     # Dropbox integrations
 │   ├── gcal        # Google Calendar data processing
 │   ├── gsheets     # Google Sheets integrations
 │   └── indexa      # Indexa Capital data extraction
 ├── schedules       # Prefect scheduling logic
 │   └─── hourly.py  # Main schedule triggering all jobs hourly
 └── vdbt            # dbt project for data modeling
```

## 🚀 How to Run the Pipeline Locally

> **Note**  
> This project uses **UV** for dependency management and execution.

### Initial Setup

1. **Install UV** (if not installed):
   ```bash
   pip install uv
   ```
2. **Set up the virtual environment:**
   ```bash
   uv venv .venv
   ```
3. **Install dependencies (in editable mode for local development):**
   ```bash
   uv pip install --editable .
   ```
4. **Ensure prek hooks are installed:**
   ```bash
   prek install
   ```

### Update deps

Update dependencies with:
```bash
uv lock --upgrade
uv sync
```

### Running Prefect Schedules

To run the main **hourly** schedule manually:
```bash
uv run python -m vtasks.schedules.hourly
```

### Running Individual Jobs (Subflows)

To run a specific job, use **module-based execution**:
```bash
uv run python -m vtasks.jobs.dropbox.export_tables
```
This ensures that **relative imports** work correctly.

## 📦 Deployment

> **Note**
> This pipeline is deployed manually to my NAS since I prefer not to grant GitHub Actions access to it. This ensures better control and security over the deployment process.

### Steps for Manual Deployment:

1. Ensure you are connected to Tailscale.
2. Set the Prefect API URL to point to the NAS.
3. Deploy all flows manually using Prefect.

```bash
set PREFECT_API_URL=http://tnas:6006/api
uv run prefect --no-prompt deploy --all
```

### 🔢 Versioning

We follow [semver](https://semver.org/) for the versioning of the project.
In short the version consists of three numbers (M.m.p) where:

* M: major version. Represents breaking changes
* m: minor version. Represents relevant changes
* p: patch version. For fixes without new functionality

You can update those by running:

```bash
uvx bump-my-version bump major
uvx bump-my-version bump minor
uvx bump-my-version bump patch
```

This automates version updates across `prefect.yaml`, `pyproject.toml`, and `vtasks/vdbt/dbt_project.yml`.

## 👤 Author
[Arnau Villoro](https://villoro.com)

## 📜 License
This repository is licensed under [MIT](https://opensource.org/licenses/MIT).

