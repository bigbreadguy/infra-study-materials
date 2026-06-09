from __future__ import annotations

from typing import Any

try:
    from google.api_core.exceptions import PreconditionFailed
except ImportError:
    PreconditionFailed = None


def candidate_object_name(object_name: str, attempt: int) -> str:
    if attempt < 0:
        raise ValueError("attempt must be greater than or equal to zero")
    if attempt == 0:
        return object_name

    directory, separator, filename = object_name.rpartition("/")
    stem, extension_separator, extension = filename.rpartition(".")

    if extension_separator and stem:
        candidate_filename = f"{stem}-{attempt:03d}.{extension}"
    else:
        candidate_filename = f"{filename}-{attempt:03d}"

    if separator:
        return f"{directory}/{candidate_filename}"

    return candidate_filename


def _is_precondition_failed(exc: Exception) -> bool:
    if PreconditionFailed is not None and isinstance(exc, PreconditionFailed):
        return True

    return type(exc).__name__ == "PreconditionFailed"


def _upload_object_if_absent(
    gcs_hook: Any,
    *,
    bucket_name: str,
    object_name: str,
    data: str,
    mime_type: str,
) -> None:
    client = gcs_hook.get_conn()
    blob = client.bucket(bucket_name).blob(object_name)
    blob.upload_from_string(
        data,
        content_type=mime_type,
        if_generation_match=0,
    )


def _validate_upload_inputs(
    *,
    bucket_name: str,
    object_name: str,
    max_attempts: int,
) -> None:
    if not bucket_name:
        raise ValueError("bucket_name must be a non-empty string")
    if not object_name:
        raise ValueError("object_name must be a non-empty string")
    if max_attempts <= 0:
        raise ValueError("max_attempts must be greater than zero")


def upload_unique_object(
    gcs_hook: Any,
    *,
    bucket_name: str,
    object_name: str,
    data: str,
    mime_type: str,
    max_attempts: int = 1000,
) -> str:
    _validate_upload_inputs(
        bucket_name=bucket_name,
        object_name=object_name,
        max_attempts=max_attempts,
    )

    for attempt in range(max_attempts):
        candidate = candidate_object_name(object_name, attempt)
        try:
            _upload_object_if_absent(
                gcs_hook,
                bucket_name=bucket_name,
                object_name=candidate,
                data=data,
                mime_type=mime_type,
            )
        except Exception as exc:
            if _is_precondition_failed(exc):
                continue
            raise

        return candidate

    raise RuntimeError(
        "Could not find an available GCS object name after "
        f"{max_attempts} attempts for gs://{bucket_name}/{object_name}"
    )
