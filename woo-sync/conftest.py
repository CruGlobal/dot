"""
Pytest configuration for woo-sync tests.

main.py instantiates a module-level BigQueryClient, which calls
google.auth.default() at import time and needs GCP credentials. Patch it before
main.py is imported so tests run locally / in CI without credentials (mirrors the
dbt-webhook conftest pattern).
"""
import os
import sys
from unittest import mock

# Repo root on sys.path so `dot_shared` imports resolve (the image gets this
# from PYTHONPATH=/app).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.auth.credentials import AnonymousCredentials

with mock.patch.dict(
    "os.environ",
    {
        "GOOGLE_CLOUD_PROJECT": "test-project",
        "BIGQUERY_PROJECT_NAME": "test-project",
        "BIGQUERY_DATASET_NAME": "test_dataset",
    },
):
    with mock.patch(
        "google.auth.default",
        return_value=(AnonymousCredentials(), "test-project"),
    ):
        # Clear any cached module so it re-imports under the mocked credentials.
        if "main" in sys.modules:
            del sys.modules["main"]
        import main  # noqa: F401
