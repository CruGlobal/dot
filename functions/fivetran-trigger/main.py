import functions_framework

import os
import json

from markupsafe import escape
from requests import request, auth, Session

# Create a global HTTP session (which provides connection pooling)
session = Session()
basic_auth = None


def init():
    global basic_auth
    basic_auth = auth.HTTPBasicAuth(env_var("API_KEY"), env_var("API_SECRET"))


def env_var(name):
    return os.environ[name]

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and "name" in request_json:
        name = request_json["name"]
    elif request_args and "name" in request_args:
        name = request_args["name"]
    else:
        name = "World"
    message = f"Hello {escape(name)}!"
    endpoint = "anything"
    url = f"https://httpbin.org/{endpoint}"
    payload = {"message": message}
    resp = session.request(
        "POST",
        url,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json;version=2",
        },
        json=payload,
        auth=basic_auth,
    )

    if resp.ok:
        data = resp.json().get("data")
        returned_message = json.loads(data)["message"]
        return returned_message
    else:
        print(f"Bad response: {resp}")
        return "that didn't work"


init()
