import os

import flask
import pytest
import responses

import main


# Create a fake "app" for generating test request contexts.
@pytest.fixture(scope="module")
def app():
  return flask.Flask(__name__)


@responses.activate
def test_http_hello(app):
  responses.add(
    responses.POST, "https://httpbin.org/anything", json={"data": "{\"message\":\"Hello World\"}"}, status=200
  )
  os.environ["API_KEY"] = "my_key"
  os.environ["API_SECRET"] = "my_secret"
  with app.test_request_context():
    main.hello_http(flask.request)

