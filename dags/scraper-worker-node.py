from __future__ import annotations

import json
from datetime import datetime
from typing import Any

# pyrefly: ignore [missing-import]
from airflow.models import Variable
# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, task


DEFAULT_ACTION_PLAN_VARIABLE = "scraper_worker_action_plan"
SUPPORTED_BROWSERS = {"chromium", "firefox", "webkit"}


def _logical_date_from_context() -> datetime:
    # pyrefly: ignore [missing-import]
    from airflow.sdk import get_current_context

    context = get_current_context()
    logical_date = context.get("logical_date")

    if not isinstance(logical_date, datetime):
        raise TypeError("Airflow context logical_date must be a datetime")

    return logical_date


def _action_plan_variable_key() -> str:
    # Keep run-specific private inputs in Airflow metadata or a secrets backend, not git.
    # pyrefly: ignore [missing-import]
    from airflow.sdk import get_current_context

    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    variable_key = conf.get("action_plan_variable", DEFAULT_ACTION_PLAN_VARIABLE)

    if not isinstance(variable_key, str) or not variable_key.strip():
        raise ValueError("dag_run.conf.action_plan_variable must be a non-empty string")

    return variable_key.strip()


def _logical_date_placeholders(logical_date: datetime) -> dict[str, str | int]:
    return {
        "{{ logical_date }}": logical_date.isoformat(),
        "{{ logical_date.date }}": logical_date.date().isoformat(),
        "{{ logical_date.day }}": logical_date.day,
        "{{ logical_date.day_padded }}": f"{logical_date.day:02d}",
        "{{ logical_date.month }}": logical_date.month,
        "{{ logical_date.month_padded }}": f"{logical_date.month:02d}",
        "{{ logical_date.year }}": logical_date.year,
        "{{ day }}": logical_date.day,
        "{{ day_padded }}": f"{logical_date.day:02d}",
        "{{ month }}": logical_date.month,
        "{{ month_padded }}": f"{logical_date.month:02d}",
        "{{ year }}": logical_date.year,
    }


def _render_logical_date_placeholders(
    value: Any, logical_date: datetime
) -> Any:
    placeholders = _logical_date_placeholders(logical_date)

    if isinstance(value, str):
        if value in placeholders:
            return placeholders[value]

        rendered_value = value
        for placeholder, replacement in placeholders.items():
            rendered_value = rendered_value.replace(placeholder, str(replacement))
        return rendered_value

    if isinstance(value, list):
        return [
            _render_logical_date_placeholders(item, logical_date)
            for item in value
        ]

    if isinstance(value, dict):
        return {
            key: _render_logical_date_placeholders(item, logical_date)
            for key, item in value.items()
        }

    return value


def _load_action_plan(variable_key: str) -> dict[str, Any]:
    raw_action_plan = Variable.get(variable_key)

    try:
        action_plan = json.loads(raw_action_plan)
    except json.JSONDecodeError as exc:
        raise ValueError("Action plan Airflow Variable must contain JSON") from exc

    if not isinstance(action_plan, dict):
        raise ValueError("Action plan Airflow Variable must contain a JSON object")

    return action_plan


def _load_rendered_action_plan(
    variable_key: str, logical_date: datetime
) -> dict[str, Any]:
    return _render_logical_date_placeholders(
        _load_action_plan(variable_key),
        logical_date,
    )


def _validated_action_plan(action_plan: dict[str, Any]) -> dict[str, Any]:
    entrypoint_url = action_plan.get("entrypoint_url")
    actions = action_plan.get("actions")
    browser_name = action_plan.get("browser_name", "chromium")
    headless = action_plan.get("headless", True)
    sleep_time = action_plan.get("sleep_time", 0.5)

    if not isinstance(entrypoint_url, str) or not entrypoint_url.strip():
        raise ValueError("Action plan requires a non-empty entrypoint_url string")
    if not isinstance(actions, list) or not actions:
        raise ValueError("Action plan requires a non-empty actions list")
    if not isinstance(browser_name, str) or browser_name not in SUPPORTED_BROWSERS:
        raise ValueError(
            "Action plan browser_name must be one of: "
            f"{', '.join(sorted(SUPPORTED_BROWSERS))}"
        )
    if not isinstance(headless, bool):
        raise ValueError("Action plan headless must be a boolean")
    if isinstance(sleep_time, bool) or not isinstance(sleep_time, int | float):
        raise ValueError("Action plan sleep_time must be a non-negative number")
    if sleep_time < 0:
        raise ValueError("Action plan sleep_time must be a non-negative number")

    return {
        "entrypoint_url": entrypoint_url,
        "actions": actions,
        "browser_name": browser_name,
        "headless": headless,
        "sleep_time": float(sleep_time),
    }


with DAG(
    dag_id="scraper-worker-node",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "worker-node", "local"],
) as dag:

    @task()
    def validate_action_plan() -> dict[str, int | str]:
        variable_key = _action_plan_variable_key()
        logical_date = _logical_date_from_context()
        action_plan = _validated_action_plan(
            _load_rendered_action_plan(variable_key, logical_date)
        )

        return {
            "action_count": len(action_plan["actions"]),
            "browser_name": action_plan["browser_name"],
            "logical_month": logical_date.month,
            "logical_year": logical_date.year,
        }

    @task()
    def run_scraper() -> dict[str, int | str]:
        try:
            # pyrefly: ignore [missing-import]
            from src.runner import run
        except ImportError as exc:
            raise RuntimeError(
                "Cannot import the local scraper package. Check the local Compose "
                "override mount and PYTHONPATH."
            ) from exc

        variable_key = _action_plan_variable_key()
        logical_date = _logical_date_from_context()
        action_plan = _validated_action_plan(
            _load_rendered_action_plan(variable_key, logical_date)
        )
        result = run(
            entrypoint_url=action_plan["entrypoint_url"],
            actions=action_plan["actions"],
            browser_name=action_plan["browser_name"],
            headless=action_plan["headless"],
            sleep_time=action_plan["sleep_time"],
        )

        return {
            "action_count": len(action_plan["actions"]),
            "browser_name": action_plan["browser_name"],
            "result_type": type(result).__name__,
        }

    validate_action_plan() >> run_scraper()
