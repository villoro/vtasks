# Changelog

All notable changes to this project will be documented here.

## [7.0.0] - 2025-02-19 🏠 Migrated to NAS & Prefect 2.20
- **Infrastructure:** Moved execution from **Heroku** to a **TerraMaster NAS** for better control and cost efficiency.
- **Prefect Upgrade:** Updated from **Prefect 2.7.1** to **2.20**, benefiting from improved scheduling and performance.
- **Improved Deployment:** Adapted deployment strategy to run on a self-hosted environment.

## [6.0.0] - 2023-11-13 🔄 CI for Version Updates *(No Breaking Changes)*
- **Continuous Integration:** Implemented **CI automation** to handle dependency version updates automatically.

## [5.0.0] - 2022-12-11 ☁️ Migrated to Heroku & Prefect 2
- **Prefect Upgrade:** Migrated from **Prefect 1.1** to **Prefect 2.7.1**, taking advantage of a revamped API and improved task orchestration.
- **Infrastructure Change:** Moved execution from **local setup** to **Heroku** for easier scalability and automation.

## [4.0.0] - 2022-12-06 🐍 Python & Poetry Upgrades
- **Python Upgrade:** Moved from **Python 3.9** to **Python 3.11**, improving performance and compatibility.
- **Poetry Upgrade:** Upgraded **Poetry from 1.1 to 1.2** for better dependency management.
- **Prefect Upgrade:** Transitioned from **Prefect 0.13** to **Prefect 1.1**.

## [3.0.0] - 2020-10-09 ⚡ Migration to Prefect
- **Switched from Luigi to Prefect:** Adopted **Prefect 0.13** for a more flexible and Pythonic workflow orchestration.
- **Improved Observability:** Leveraged **Prefect UI** for monitoring and debugging.

## [2.0.0] - 2020-05-10 📝 Custom Luigi Task Automation
- **Task Automation:** Implemented **custom Luigi tasks** to avoid manual file handling.
- **YAML-based Workflow Tracking:** Tasks now **export results automatically to YAML**, simplifying result storage and logging.

## [1.0.0] - 2020-01-03 💻 Migration to Luigi (Locally for Cost Savings)
- **Switched from Airflow to Luigi 2.8:** Simplified local execution and reduced infrastructure overhead.
- **Local Execution:** Fully transitioned from **AWS-managed workflows** to **local execution** for better iteration speed.

## [0.1.0] - 2019-04-21 🌍 Initial Release (Airflow + AWS)
- **Orchestrated with Airflow:** Scheduled workflows using **Apache Airflow**.
- **AWS-Hosted:** The pipeline was running on **AWS**, leveraging managed infrastructure.
