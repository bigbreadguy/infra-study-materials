# Infra Study Materials

This repository contains a local Airflow cluster configuration for infrastructure study material.

## Airflow DAGs

Project DAGs live in `dags/` and are mounted into Airflow at `/opt/airflow/dags`.

Airflow bundled example DAGs are disabled for cluster initialization:

- `docker-compose.yaml` sets `AIRFLOW__CORE__LOAD_EXAMPLES` to `false`.
- `config/airflow.cfg` sets `load_examples` to `False`.

With a fresh metadata database, Airflow should load only DAGs from the repository `dags/` directory.
If the cluster was already initialized before this setting changed, stale example DAG metadata may remain until the Airflow metadata database is cleaned or recreated.

## Local Private DAG Inputs

This public repository can run DAGs that import local-only code without committing that code or its paths.

- Keep private DAGs under ignored paths such as `dags/private/` or `dags/local/`.
- Keep local source mounts in an ignored Compose override file.
- Mount only the importable source tree read-only, and set `PYTHONPATH` to the mounted parent directory.
- Keep credentials, target URLs, selectors, and private response payloads in Airflow Connections, Airflow Variables, a secrets backend, or ignored env files.

The Airflow image is built from `Dockerfile` so runtime dependencies are repeatable. Rebuild it after changing `requirements/airflow-runtime.txt`.
Leave `_PIP_ADDITIONAL_REQUIREMENTS` unset or empty for normal runs; use it only for temporary experiments.

## Worker Node Scraper DAG

The `scraper-worker-node` DAG runs the local scraper package on an Airflow Celery worker. Store the action plan as an Airflow Variable named `scraper_worker_action_plan`, or trigger the DAG with `{"action_plan_variable": "scraper_worker_action_plan"}` to use another generic variable key.
The local Airflow image installs only the Chromium Playwright browser, so set `browser_name` to `chromium`.

The action plan may reference Airflow's `logical_date` with placeholders. Use `{{ year }}` and `{{ month }}` for the same year/month values used by the local scraper script.

Example placeholder shape:

```json
{
  "entrypoint_url": "https://example.invalid",
  "browser_name": "chromium",
  "headless": true,
  "sleep_time": 0.5,
  "actions": [
    {"select_option": {"selector": "title=example-year", "value": "{{ year }}"}},
    {"select_month_option": {"selector": "title=example-month", "month": "{{ month }}"}},
    {"check_month_box": {"start_year": "{{ year }}", "month": "{{ month }}"}},
    {"return_stats": {}}
  ]
}
```

Do not commit real target URLs, selectors, headers, cookies, credentials, response payloads, or private action plans. Use Airflow Variables, Airflow Connections, a secrets backend, or ignored local files for those values.
