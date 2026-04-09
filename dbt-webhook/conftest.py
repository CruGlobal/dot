"""
Pytest configuration for dbt-webhook tests.

The PublisherClient is instantiated at module level in main.py, which requires
GCP credentials. This conftest patches it before main.py is imported so tests
can run locally without credentials.

Each topic gets a distinct mock path so tests can verify routing correctness
(e.g., success events go to the completed topic, not the retry topic).
"""
import sys
from unittest import mock


def mock_topic_path(project, topic_name):
    """Return a distinct mock path per topic so routing assertions are meaningful."""
    return f"projects/{project}/topics/{topic_name}"


mock_publisher = mock.MagicMock()
mock_publisher.topic_path.side_effect = mock_topic_path

with mock.patch.dict(
    "os.environ",
    {"GOOGLE_CLOUD_PROJECT": "test-project", "DBT_WEBHOOK_SECRET": "test-secret"},
):
    with mock.patch(
        "google.cloud.pubsub_v1.PublisherClient", return_value=mock_publisher
    ):
        # Clear cached module so it re-imports with mocked credentials.
        # Necessary for pytest-watch or other re-run-in-same-process tools.
        if "main" in sys.modules:
            del sys.modules["main"]
        import main
