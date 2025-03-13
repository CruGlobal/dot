import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore
import os

fivetran_api_key = os.environ.get("FIVETRAN_API_KEY", None)
fivetran_api_secret = os.environ.get("FIVETRAN_API_SECRET", None)
fivetran_webhook_secret = os.environ.get("FIVETRAN_WEBHOOK_SECRET", None)
a = HTTPBasicAuth(fivetran_api_key, fivetran_api_secret)
# create new webhook for a given group


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


def create_group_webhook():
    # Request
    group_id = "contradiction_cosmic"  # For BigQueryELT destination
    method = "POST"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks/group/" + group_id
    payload = {
        "url": "https://webhook-handler-289484875682.us-central1.run.app",
        "events": ["sync_start", "sync_end"],
        "secret": fivetran_webhook_secret,
        "active": True,
    }

    # Submit
    response = atlas(method, endpoint, payload)
    print(response)
    # Review
    if response is not None:
        print(Fore.CYAN + "Call: " + method + " " + endpoint + " " + str(payload))
        print(Fore.GREEN + "Response: " + response["code"])
        print(Fore.MAGENTA + str(response))


def create_account_webhook():
    # Request
    method = "POST"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks/account"
    payload = {
        "url": "https://webhook-handler-289484875682.us-central1.run.app",
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
        print(Fore.CYAN + "Call: " + method + " " + endpoint + " " + str(payload))
        print(Fore.GREEN + "Response: " + response["code"])
        print(Fore.MAGENTA + str(response))


def list_webhooks():
    # Request
    method = "GET"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "webhooks"
    payload = {}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print(Fore.CYAN + "Call: " + method + " " + endpoint + " " + str(payload))
        print(Fore.GREEN + "Response: " + response["code"])
        print(Fore.MAGENTA + str(response))


def list_groups():
    # Request
    method = "GET"  #'POST' 'PATCH' 'DELETE' 'GET'
    endpoint = "groups"
    payload = {}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print(Fore.CYAN + "Call: " + method + " " + endpoint + " " + str(payload))
        print(Fore.GREEN + "Response: " + response["code"])
        print(Fore.MAGENTA + str(response))


def test_webhook():
    # Request
    method = "POST"  #'POST' 'PATCH' 'DELETE' 'GET'
    webhook_id = "exhale_landmine"
    endpoint = "webhooks/" + webhook_id + "/test"
    payload = {"event": "sync_end"}

    # Submit
    response = atlas(method, endpoint, payload)
    # Review
    if response is not None:
        print(Fore.CYAN + "Call: " + method + " " + endpoint + " " + str(payload))
        print(Fore.GREEN + "Response: " + response["code"])
        print(Fore.MAGENTA + str(response))


# create_group_webhook()
# create_account_webhook()
# list_groups()
test_webhook()
