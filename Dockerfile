ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.2.2

FROM ${AIRFLOW_BASE_IMAGE}

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libgbm1 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libx11-6 \
        libx11-xcb1 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
        xdg-utils \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}" \
    && chown -R airflow:0 "${PLAYWRIGHT_BROWSERS_PATH}" \
    && chmod -R g=u "${PLAYWRIGHT_BROWSERS_PATH}"

USER airflow

COPY requirements/airflow-runtime.txt /requirements/airflow-runtime.txt

RUN pip install --no-cache-dir -r /requirements/airflow-runtime.txt \
    && python -m playwright install chromium

USER root

RUN chmod -R g=u "${PLAYWRIGHT_BROWSERS_PATH}"

USER airflow
