# Infra Study Materials

This repository contains a local Airflow cluster configuration for infrastructure study material.

## Airflow DAGs

Project DAGs live in `dags/` and are mounted into Airflow at `/opt/airflow/dags`.

Airflow bundled example DAGs are disabled for cluster initialization:

- `docker-compose.yaml` sets `AIRFLOW__CORE__LOAD_EXAMPLES` to `false`.
- `config/airflow.cfg` sets `load_examples` to `False`.

With a fresh metadata database, Airflow should load only DAGs from the repository `dags/` directory.
If the cluster was already initialized before this setting changed, stale example DAG metadata may remain until the Airflow metadata database is cleaned or recreated.
