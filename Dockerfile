ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.2.2

FROM ${AIRFLOW_BASE_IMAGE}

# Provider packages only. The actual scraping workload is outsourced to a Cloud Run
# Job (triggered by the scrape-external-data DAG), so the image no longer ships a
# browser engine or its system libraries.
COPY requirements/airflow-runtime.txt /requirements/airflow-runtime.txt

RUN AIRFLOW_VERSION="$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))')" \
    && pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /requirements/airflow-runtime.txt \
        --constraint "${HOME}/constraints.txt"
