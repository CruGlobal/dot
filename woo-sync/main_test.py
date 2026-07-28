import logging
import sys
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses

from main import (
    CloudLoggingFormatter,
    backfill_orders_by_id,
    get_last_load_date_time,
    get_orders_and_items,
)


@pytest.fixture(autouse=True)
def setup_logging():
    """Configure the primary logger the same way main.py does, for tests."""
    logger = logging.getLogger("primary_logger")
    logger.handlers = []
    logger.propagate = True

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(CloudLoggingFormatter(fmt="%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield

    logger.handlers = []
    logger.propagate = False


class _Row:
    """Stand-in for a BigQuery result row with a sync_timestamp attribute."""

    def __init__(self, sync_timestamp):
        self.sync_timestamp = sync_timestamp


def _mock_bq_client(rows=None, query_error=None):
    """Build a mock BigQuery client whose query().result() yields ``rows``."""
    client = mock.MagicMock()
    if query_error is not None:
        client.query.side_effect = query_error
    else:
        job = mock.MagicMock()
        job.result.return_value = rows or []
        client.query.return_value = job
    return client


def _orders_env(url="http://woo.test/orders", watermark="2026-07-08T13:46:01Z"):
    return {
        "order_last_update_date_time": watermark,
        "orders_api_url": url,
        "woo_api_client_id": "test-id",
        "woo_api_client_secret": "test-secret",
        "store_wid": "1",
        "rls_value": "cru_woo",
        "sync_timestamp": "2026-07-08 13:46:01+00:00",
    }


# --- get_last_load_date_time: fail-loud watermark (DT-594) --------------------

def test_watermark_happy_path_returns_iso_string():
    client = _mock_bq_client(rows=[_Row("2026-07-08 13:46:01")])
    result = get_last_load_date_time("SELECT 1", client=client)
    assert result == "2026-07-08T13:46:01Z"


def test_watermark_zero_rows_raises_instead_of_returning_none():
    """The core DT-594 fix: 0 rows must raise, never fall through to None."""
    client = _mock_bq_client(rows=[])
    with pytest.raises(ValueError, match="0 rows"):
        get_last_load_date_time("SELECT 1", client=client)


def test_watermark_query_error_propagates():
    client = _mock_bq_client(query_error=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        get_last_load_date_time("SELECT 1", client=client)


# --- get_orders_and_items: guards + fail-loud HTTP ---------------------------

def test_orders_empty_watermark_refuses_full_pull():
    """Defense-in-depth: an empty watermark must not reach the API as None."""
    with pytest.raises(ValueError, match="modified_after"):
        get_orders_and_items(_orders_env(watermark=""))


@responses.activate
def test_orders_non_200_raises():
    """A non-200 from the Woo API must fail loud, not silently skip the page."""
    url = "http://woo.test/orders"
    responses.add(responses.GET, url, status=500)
    with pytest.raises(requests.exceptions.HTTPError):
        get_orders_and_items(_orders_env(url=url))


# --- backfill_orders_by_id: surgical include= re-pull (DT-594 follow-up) -----

def _fake_orders(o, order_list, env_var_list):
    order_list.append(o)


def _fake_order_items(o, order_item_list, env_var_list):
    order_item_list.append(o)


def _backfill_env(url="http://woo.test/orders"):
    return {
        "orders_api_url": url,
        "woo_api_client_id": "test-id",
        "woo_api_client_secret": "test-secret",
    }


@responses.activate
def test_backfill_chunks_by_batch_size_and_commits_incrementally():
    """250 IDs at batch_size=100 -> 3 chunks (100, 100, 50); commit_every=2 means
    one commit after chunk 2 and a final flush for the trailing chunk 3."""
    order_ids = list(range(1, 251))
    for _ in range(3):
        responses.add(responses.GET, "http://woo.test/orders", json=[{"id": 1}], status=200)

    with mock.patch("main.orders", side_effect=_fake_orders), \
         mock.patch("main.order_items", side_effect=_fake_order_items), \
         mock.patch("main.process_orders") as mock_process_orders, \
         mock.patch("main.process_order_items") as mock_process_order_items:
        backfill_orders_by_id(order_ids, _backfill_env(), batch_size=100, commit_every=2)

    assert len(responses.calls) == 3
    include_lens = [
        len(parse_qs(urlparse(call.request.url).query)["include"][0].split(","))
        for call in responses.calls
    ]
    assert include_lens == [100, 100, 50]
    assert mock_process_orders.call_count == 2
    assert mock_process_order_items.call_count == 2


@responses.activate
def test_backfill_chunk_failure_does_not_abort_the_run():
    """A failed chunk (unlike the daily sync) must not raise -- the remaining
    chunks still get pulled and committed."""
    order_ids = list(range(1, 201))
    responses.add(responses.GET, "http://woo.test/orders", status=500)
    responses.add(responses.GET, "http://woo.test/orders", json=[{"id": 1}], status=200)

    with mock.patch("main.orders", side_effect=_fake_orders), \
         mock.patch("main.order_items", side_effect=_fake_order_items), \
         mock.patch("main.process_orders") as mock_process_orders, \
         mock.patch("main.process_order_items") as mock_process_order_items:
        backfill_orders_by_id(order_ids, _backfill_env(), batch_size=100, commit_every=5)

    assert len(responses.calls) == 2
    mock_process_orders.assert_called_once_with([{"id": 1}])
    mock_process_order_items.assert_called_once_with([{"id": 1}])
