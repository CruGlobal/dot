"""
Mock GCP credentials and Pub/Sub client before main.py is imported.
The PublisherClient() is instantiated at module level in main.py,
which requires GCP credentials. This conftest patches it before any
test module imports main.
"""
import sys
from unittest import mock

# Patch the Pub/Sub client before main.py is imported
mock_publisher = mock.MagicMock()
mock_publisher.topic_path.return_value = "projects/test-project/topics/mock-topic"

with mock.patch.dict(
    "os.environ",
    {"GOOGLE_CLOUD_PROJECT": "test-project", "DBT_WEBHOOK_SECRET": "test-secret"},
):
    with mock.patch("google.cloud.pubsub_v1.PublisherClient", return_value=mock_publisher):
        if "main" in sys.modules:
            del sys.modules["main"]
        import main
