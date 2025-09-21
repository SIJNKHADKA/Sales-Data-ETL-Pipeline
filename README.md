# Sales-Data-ETL-Pipeline

## Overview
This project is an **Airflow ETL pipeline** for processing sales data. The pipeline is fully **in-memory** and performs the following steps:

1. **Extract:** Downloads sales data from a public URL (or uses sample data if the URL is unavailable).
2. **Transform:** Cleans, transforms, and engineers new features such as `TOTAL_REVENUE`, `EST_PROFIT`, and `HIGH_VALUE_ORDER`.
3. **Load:** Loads the transformed dataset directly into a **PostgreSQL** database.
4. **Validate:** Runs validation queries on the loaded data.
5. **Test Connection:** Ensures the PostgreSQL connection is working before loading data.

The main DAG is implemented in `sales_data.py`.

---

## Features
- **Airflow DAG** using the `@dag` and `@task` decorators.
- Data transformation entirely in memory using **pandas**.
- Supports PostgreSQL loading via `PostgresHook`.
- Automatic table creation and data type handling.
- Basic validation queries to ensure data integrity.

---

## Requirements
- Python 3.8+
- [Apache Airflow](https://airflow.apache.org/) 2.x
- Airflow PostgreSQL provider: `apache-airflow-providers-postgres`
- pandas
- requests

You can install the dependencies using pip:

```bash
pip install apache-airflow pandas requests apache-airflow-providers-postgres
