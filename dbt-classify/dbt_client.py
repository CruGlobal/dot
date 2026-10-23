import logging
import os
import requests

logger = logging.getLogger("primary_logger")

# Our account-specific dbt Cloud host (cloud.getdbt.com is retired). Set DBT_BASE_URL to
# override, as the bare host or with /api/v2: dse-dbt-jobs-as-code uses the bare host,
# while the shared python package and dse-portal include /api/v2.
DEFAULT_DBT_BASE_URL = "https://kw745.us1.dbt.com/api/v2"


def dbt_api_base() -> str:
    """The dbt Cloud API v2 base URL, without a trailing slash."""
    base = (os.environ.get("DBT_BASE_URL") or DEFAULT_DBT_BASE_URL).rstrip("/")
    return base if base.endswith("/api/v2") else f"{base}/api/v2"


# (connect, read) timeouts in seconds. The failures this client classifies (rate
# limits, 5xx, infra) are exactly when dbt Cloud may hang a socket, so fail fast on a
# dead connection; allow a longer read for run_results.json, which is multi-MB on a
# large job (the 354-node case). Without a timeout a stalled read blocks until the
# Cloud Function platform timeout, stalling the invoking retry workflow.
HTTP_TIMEOUT = (10, 60)


class DbtReadClient:
    """Read-only dbt Cloud API client for the classifier: fetch a failed run's
    metadata (trigger + run steps) and its run_results.json artifact."""

    def __init__(self, access_token: str, account_id: str):
        self.account_id = account_id
        # dbt Cloud service tokens authenticate as "Token <key>" (POC-validated for
        # these read endpoints).
        self.headers = {"Authorization": f"Token {access_token}"}
        self.account_url = f"{dbt_api_base()}/accounts/{account_id}"

    def get_run(self, run_id: str) -> dict:
        """GET the run including its trigger (for the loop-guard cause) and run_steps.
        Returns the `data` object; raises on a non-2xx response."""
        resp = requests.get(
            f"{self.account_url}/runs/{run_id}/",
            params={"include_related": '["trigger","run_steps"]'},
            headers=self.headers,
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("data", {})

    def get_run_results(self, run_id: str) -> list:
        """GET the run_results.json artifact. Returns the results[] list (possibly
        empty); raises on a non-2xx response (e.g. artifact not produced)."""
        resp = requests.get(
            f"{self.account_url}/runs/{run_id}/artifacts/run_results.json",
            headers=self.headers,
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
