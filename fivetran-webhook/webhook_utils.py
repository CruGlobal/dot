import requests
from requests.auth import HTTPBasicAuth
import os

fivetran_api_key = os.environ.get("FIVETRAN_API_KEY", None)
fivetran_api_secret = os.environ.get("FIVETRAN_API_SECRET", None)
fivetran_webhook_secret = os.environ.get("FIVETRAN_WEBHOOK_SECRET", None)
gateway_stage_url = (
    "https://fivetran-webhook-gateway-6cr7oo3w.uc.gateway.dev/fivetran-webhook"
)
gateway_prod_rul = (
    "https://fivetran-webhook-gateway-6sk89xvx.uc.gateway.dev/fivetran-webhook"
)

a = HTTPBasicAuth(fivetran_api_key, fivetran_api_secret)


def atlas(method, endpoint, payload):

    base_url = "https://api.fivetran.com/v1"
    h = {
        "Authorization": f"Bearer {fivetran_api_key}:{fivetran_api_secret}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{base_url}/{endpoint}"

    try:
        if method == "GET":
            response = requests.get(url, headers=h, auth=a)
        elif method == "POST":
            response = requests.post(url, headers=h, json=payload, auth=a)
        elif method == "PATCH":
            response = requests.patch(url, headers=h, json=payload, auth=a)
        elif method == "DELETE":
            response = requests.delete(url, headers=h, auth=a)
        else:
            raise ValueError("Invalid request method.")

        response.raise_for_status()  # Raise exception

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def create_group_webhook(url):
    # Request
    group_id = "contradiction_cosmic"  # For BigQueryELT destination
    method = "POST"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks/group/" + group_id
    payload = {
        "url": url,
        "events": ["sync_start", "sync_end"],
        "secret": fivetran_webhook_secret,
        "active": True,
    }

    # Submit
    response = atlas(method, endpoint, payload)
    print(response)
    # Review
    if response is not None:
        print("Call: " + method + " " + endpoint + " " + str(payload))
        print("Response: " + response["code"])
        print(str(response))


def create_account_webhook(url):
    # Request
    method = "POST"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks/account"
    payload = {
        "url": url,
        "events": [
            "sync_start",
            "sync_end",
            "transformation_run_start",
            "transformation_run_succeeded",
        ],
        "active": True,
        "secret": fivetran_webhook_secret,
    }

    response = atlas(method, endpoint, payload)
    print(response)
    if response is not None:
        print("Call: " + method + " " + endpoint + " " + str(payload))
        print("Response: " + response["code"])
        print(str(response))


def list_webhooks():
    # Request
    method = "GET"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks"
    payload = {}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print("Call: " + method + " " + endpoint + " " + str(payload))
        print("Response: " + response["code"])
        print(str(response))


def list_groups():
    # Request
    method = "GET"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "groups"
    payload = {}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print("Call: " + method + " " + endpoint + " " + str(payload))
        print("Response: " + response["code"])
        print(str(response))


def delete_webhook(webhook_id):
    # Request
    method = "DELETE"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks/" + webhook_id
    payload = {}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print("Call: " + method + " " + endpoint + " " + str(payload))
        print("Response: " + response["code"])
        print(str(response))


def test_webhook(webhook_id):
    # Request
    method = "POST"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks/" + webhook_id + "/test"
    payload = {"event": "sync_end"}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print("Call: " + method + " " + endpoint + " " + str(payload))
        print("Response: " + response["code"])
        print(str(response))


# create_account_webhook(gateway_prod_rul)
test_webhook("outage_posting")
# list_webhooks()
# delete_webhook("untenable_manganese")
