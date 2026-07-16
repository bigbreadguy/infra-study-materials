"""Trigger the generic scraper Cloud Run Job and gate on its exit, via ADC.

This is the prod-convention replacement for the study DAG's
``CloudRunExecuteJobOperator``: there is no Airflow GCP connection on the
KubernetesExecutor cluster, so the job is run with a ``google.cloud.run_v2``
client resolving Application Default Credentials (the Airflow workload identity
service account), never ``gcp_conn_id``.

The scraper is a single recipe-agnostic job; the per-execution request is passed
as ``REQUEST_URI`` / ``OUTPUT_URI`` container env overrides (the job reads its
request object from GCS and writes its result object back). ``execute_scraper_job``
runs the job synchronously: it waits for the execution to finish and raises unless
every task succeeded, so a non-zero scrape exit fails the Airflow task -- the
single gate, no result-existence sensor (matching the study operator's
``deferrable=False`` behaviour).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any


# --- The timeout ladder ----------------------------------------------------
#
# Three bounds nest around one scrape, innermost first. Each must strictly
# outlast the one inside it, or the outer bound fires on a job that is still
# healthy and an Airflow retry launches a duplicate execution:
#
#   1. Cloud Run ``task_timeout``  (server-side; kills the container)
#   2. ``result_timeout``          (client-side; bounds the LRO poll)
#   3. Airflow ``execution_timeout`` (pod-side; kills the task instance)
#
# (1) lives in deepfl-infra as ``scraper_job_timeout_seconds`` and is mirrored
# here only to derive (2) and (3). Changing it there means changing it here.
CLOUD_RUN_TASK_TIMEOUT_SECONDS = 3600

# Cloud Run kills the task at (1), then needs a moment to report the terminal
# state back through the operation. Poll past the kill so the caller observes a
# real ``failed_count`` -- not a client-side give-up on a job that already died.
SCRAPER_RESULT_TIMEOUT_SECONDS = CLOUD_RUN_TASK_TIMEOUT_SECONDS + 300

# The outermost bound: a scrape task that has not returned by now is stuck
# somewhere the two inner bounds do not cover (GCS upload wedged, LRO poll
# livelocked). Slack over (2) so the RuntimeError from a failed execution
# surfaces in the logs rather than being pre-empted by a task SIGKILL.
SCRAPE_EXECUTION_TIMEOUT = timedelta(seconds=SCRAPER_RESULT_TIMEOUT_SECONDS + 600)

# A PACED source loops every recipe in ONE pod, so the single-scrape ceiling would
# kill it mid-loop. The strict worst case (recipes x the full LRO poll) runs to tens
# of hours for a wide source like yfinance -- a ceiling that loose never fires. Every
# recipe historically finished well inside the old 1800s Cloud Run cap, and rung (2)
# already guarantees each individual scrape terminates, so this is only a backstop for
# a pod wedged below that guarantee. Revisit if a paced source grows a genuinely slow
# recipe rather than merely many fast ones.
SERIAL_SCRAPE_EXECUTION_TIMEOUT = timedelta(hours=4)


def job_resource_name(project_id: str, region: str, job_name: str) -> str:
    """Fully-qualified Cloud Run Job resource path."""
    for label, value in (
        ("project_id", project_id),
        ("region", region),
        ("job_name", job_name),
    ):
        if not value:
            raise ValueError(f"{label} must be a non-empty string")
    return f"projects/{project_id}/locations/{region}/jobs/{job_name}"


def build_run_request(
    *,
    project_id: str,
    region: str,
    job_name: str,
    request_uri: str,
    output_uri: str,
    extra_env: dict[str, str] | None = None,
) -> Any:
    """Build the RunJobRequest with REQUEST_URI / OUTPUT_URI env overrides.

    ``extra_env`` adds further per-execution container env overrides on top of the two
    URIs -- used to inject a source's credential the scraper resolves from the
    environment (EIA's api key is managed as an Airflow Variable and passed as
    ``EIA_API_KEY`` with ``SCRAPE_LOCAL_CREDENTIALS=1``, rather than from Secret
    Manager). It cannot shadow the two URI overrides; a collision is a config bug and
    fails loudly.
    """
    if not request_uri or not output_uri:
        raise ValueError("request_uri and output_uri must be non-empty strings")
    # Deferred import keeps this module importable without google-cloud-run.
    from google.cloud import run_v2

    env = [
        run_v2.EnvVar(name="REQUEST_URI", value=request_uri),
        run_v2.EnvVar(name="OUTPUT_URI", value=output_uri),
    ]
    for name, value in (extra_env or {}).items():
        if name in ("REQUEST_URI", "OUTPUT_URI"):
            raise ValueError(f"extra_env must not override the {name} URI")
        if not name or value is None:
            raise ValueError("extra_env names must be non-empty and values non-null")
        env.append(run_v2.EnvVar(name=name, value=str(value)))

    overrides = run_v2.RunJobRequest.Overrides(
        container_overrides=[
            run_v2.RunJobRequest.Overrides.ContainerOverride(env=env)
        ],
        task_count=1,
    )
    return run_v2.RunJobRequest(
        name=job_resource_name(project_id, region, job_name),
        overrides=overrides,
    )


def execute_scraper_job(
    *,
    project_id: str,
    region: str,
    job_name: str,
    request_uri: str,
    output_uri: str,
    extra_env: dict[str, str] | None = None,
    client: Any | None = None,
    result_timeout: float = SCRAPER_RESULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run the scraper job for one recipe and block until it finishes.

    Returns the terminal execution name and task counts. Raises if any task
    failed so the Airflow task fails on a non-zero scrape exit. ``extra_env`` injects
    additional per-execution container env (e.g. a source's api key).

    ``result_timeout`` bounds the client-side LRO polling; without it,
    ``operation.result()`` gives up after google-api-core's 900s default while the
    execution keeps running server-side (and an Airflow retry would then launch a
    duplicate execution). It is rung 2 of the timeout ladder at the top of this
    module: it must outlast the Cloud Run job's own ``task_timeout`` and stay under
    the calling Airflow task's ``execution_timeout``.
    """
    if client is None:
        from google.cloud import run_v2

        client = run_v2.JobsClient()

    request = build_run_request(
        project_id=project_id,
        region=region,
        job_name=job_name,
        request_uri=request_uri,
        output_uri=output_uri,
        extra_env=extra_env,
    )
    # run_job returns a long-running operation; result() blocks until the
    # execution reaches a terminal state and raises if the operation errored.
    operation = client.run_job(request=request)
    execution = operation.result(timeout=result_timeout)

    succeeded = getattr(execution, "succeeded_count", 0)
    failed = getattr(execution, "failed_count", 0)
    task_count = getattr(execution, "task_count", 0)
    if succeeded < task_count or failed:
        raise RuntimeError(
            f"Scraper job execution {getattr(execution, 'name', job_name)} did not "
            f"succeed: {succeeded}/{task_count} tasks succeeded, {failed} failed"
        )

    return {
        "execution_name": getattr(execution, "name", None),
        "task_count": task_count,
        "succeeded_count": succeeded,
    }
