from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from common.gcs_object import candidate_object_name, upload_unique_object


class PreconditionFailed(Exception):
    pass


class FakeBlob:
    def __init__(self, bucket: "FakeBucket", object_name: str):
        self.bucket = bucket
        self.object_name = object_name

    def upload_from_string(
        self,
        data: str,
        *,
        content_type: str,
        if_generation_match: int,
    ) -> None:
        if if_generation_match != 0:
            raise ValueError("FakeBlob requires create-only upload precondition")
        if self.object_name in self.bucket.existing_objects:
            raise PreconditionFailed

        self.bucket.existing_objects.add(self.object_name)
        self.bucket.uploads.append(
            {
                "bucket_name": self.bucket.bucket_name,
                "object_name": self.object_name,
                "data": data,
                "mime_type": content_type,
                "if_generation_match": if_generation_match,
            }
        )


class FakeBucket:
    def __init__(
        self,
        bucket_name: str,
        existing_objects: set[str],
        uploads: list[dict[str, str | int]],
    ):
        self.bucket_name = bucket_name
        self.existing_objects = existing_objects
        self.uploads = uploads

    def blob(self, object_name: str) -> FakeBlob:
        return FakeBlob(self, object_name)


class FakeClient:
    def __init__(self, hook: "FakeGCSHook"):
        self.hook = hook

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(bucket_name, self.hook.existing_objects, self.hook.uploads)


class FakeGCSHook:
    def __init__(self, existing_objects: set[str] | None = None):
        self.existing_objects = existing_objects or set()
        self.uploads = []

    def get_conn(self) -> FakeClient:
        return FakeClient(self)


class GCSObjectTest(TestCase):
    def test_candidate_object_name_adds_suffix_before_extension(self):
        self.assertEqual(
            candidate_object_name("prefix/raw-19960416.ndjson", 1),
            "prefix/raw-19960416-001.ndjson",
        )

    def test_upload_unique_object_uses_next_available_name(self):
        gcs_hook = FakeGCSHook(
            {
                "prefix/raw-19960416.ndjson",
                "prefix/raw-19960416-001.ndjson",
            }
        )

        object_name = upload_unique_object(
            gcs_hook,
            bucket_name="bucket",
            object_name="prefix/raw-19960416.ndjson",
            data="payload",
            mime_type="application/x-ndjson",
        )

        self.assertEqual(object_name, "prefix/raw-19960416-002.ndjson")
        self.assertEqual(
            gcs_hook.uploads,
            [
                {
                    "bucket_name": "bucket",
                    "object_name": "prefix/raw-19960416-002.ndjson",
                    "data": "payload",
                    "mime_type": "application/x-ndjson",
                    "if_generation_match": 0,
                }
            ],
        )
