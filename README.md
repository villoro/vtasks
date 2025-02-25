# vtasks: Personal Pipeline
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

This repository contains my personal data pipeline, built with [Prefect](https://www.prefect.io/), to automate data extraction, transformation, and loading (ETL). The pipeline runs on my NAS and integrates multiple data sources, processing them and exporting the results to [MotherDuck](https://motherduck.com/) for analysis in [Metabase](https://www.metabase.com/).

## Pipeline Overview

The pipeline serves two main purposes:

1. **Automation:** Automates repetitive data tasks by extracting data from APIs, cloud services, and other sources.
2. **Analysis:** Loads transformed data into MotherDuck, where Metabase connects for visualization and insights.

## Prefect Workflow

The pipeline is orchestrated with Prefect, using flows and tasks to manage dependencies and execution. The scheduling logic is defined in `schedules/hourly.py`, ensuring that jobs run on an hourly basis. The execution happens on my NAS.

### Data Flow
1. **Extract:** Data is sourced from various integrations, including Dropbox, Google Sheets, APIs, and local files.
2. **Load:** Processed data is exported to MotherDuck for storage and analysis.
3. **Transform:** Data processing is primarily handled using `dbt`, with additional transformations as needed.
4. **Visualization:** Metabase connects to MotherDuck to generate insights and reports.

## Repository Structure

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
 ├── vdbt            # dbt transformations for data modeling
 └─── tests          # Test cases for pipeline components
```

## Deployment

> **Note**
> This pipeline is deployed manually to my NAS since I prefer not to grant GitHub Actions access to it. This ensures better control and security over the deployment process.

### Steps for Manual Deployment:

1. Ensure you are connected to Tailscale.
2. Set the Prefect API URL to point to the NAS.
3. Deploy all flows manually using Prefect.

```bash
set PREFECT_API_URL=http://tnas:6006/api
prefect --no-prompt deploy --all
```

This approach provides full control over the deployment process while maintaining security and isolation.

## Author
[Arnau Villoro](https://villoro.com)

## License
This repository is licensed under [MIT](https://opensource.org/licenses/MIT).
