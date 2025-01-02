import pytest
import responses
import pandas as pd
import io
from unittest import mock
import logging
import sys
from main import (
    get_authentication,
    load_to_dataframe,
    read_csv_from_bytes,
    process_zip_file,
    get_dtype_mapping,
    CloudLoggingFormatter,
)


@pytest.fixture
def sample_schema():
    return [
        ["column1", "string"],
        ["column2", "integer"],
        ["column3", "float"],
    ]


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("GEONAMES_USERNAME", "test_user")
    monkeypatch.setenv("GEONAMES_PASSWORD", "test_pass")
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "test_key")


@pytest.fixture(autouse=True)
def setup_logging():
    """Set up logging for tests using the same configuration as main.py"""
    logger = logging.getLogger("primary_logger")
    logger.handlers = []
    logger.propagate = True

    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = CloudLoggingFormatter(fmt="%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield

    logger.handlers = []
    logger.propagate = False


def test_get_authentication_geonames(mock_env_vars):
    url = "https://www.geonames.org/test"
    result_url, auth = get_authentication(url)
    assert result_url == url
    assert auth == ("test_user", "test_pass")


def test_get_authentication_maxmind(mock_env_vars):
    url = "https://download.maxmind.com/test"
    result_url, auth = get_authentication(url)
    assert "license_key=test_key" in result_url
    assert auth is None


def test_get_dtype_mapping():
    dtype_map = get_dtype_mapping()
    assert dtype_map["string"] == "string"
    assert dtype_map["integer"] == "Int64"
    assert dtype_map["float"] == "float64"


@mock.patch("pandas.read_csv")
def test_read_csv_from_bytes(mock_read_csv):
    mock_df = pd.DataFrame(
        {"column1": ["a", "b"], "column2": [1, 2], "column3": [1.1, 2.2]}
    )
    mock_read_csv.return_value = mock_df

    file_bytes = io.BytesIO(b"dummy,data")
    sep = ","
    skip_header_rows = 1
    header = 0
    dtypes = {0: "string", 1: "Int64", 2: "float64"}
    num_columns = 3
    na_values = ["NA", ""]

    result = read_csv_from_bytes(
        file_bytes, sep, skip_header_rows, header, dtypes, num_columns, na_values
    )

    mock_read_csv.assert_called_once()
    assert isinstance(result, pd.DataFrame)


@responses.activate
def test_load_to_dataframe(sample_schema):
    mock_content = b"column1,column2,column3\na,1,1.1\nb,2,2.2"
    responses.add(
        responses.GET, "http://test.com/data.csv", body=mock_content, status=200
    )

    df = load_to_dataframe(
        url="http://test.com/data.csv",
        schema=sample_schema,
        sep=",",
        skip_header_rows=1,
    )

    assert isinstance(df, pd.DataFrame)
    assert len(responses.calls) == 1


@mock.patch("zipfile.ZipFile")
def test_process_zip_file(mock_zipfile):
    mock_content = b"column1,column2,column3\na,1,1.1\nb,2,2.2"

    mock_file = mock.MagicMock()
    mock_file.__enter__.return_value.read.return_value = mock_content

    mock_zip = mock.MagicMock()
    mock_zip.namelist.return_value = ["test.csv"]
    mock_zip.open.return_value = mock_file
    mock_zipfile.return_value = mock_zip

    dtypes = {0: "string", 1: "Int64", 2: "float64"}
    result = process_zip_file(mock_zip, r"test\.csv", ",", 1, 0, dtypes, 3, ["NA", ""])

    assert isinstance(result, pd.DataFrame)
    assert mock_zip.namelist.call_count == 2
    mock_zip.open.assert_called_once_with("test.csv")


def test_process_zip_file_no_matching_file():
    mock_zip = mock.MagicMock()
    mock_zip.namelist.return_value = ["some_other_file.txt", "wrong.csv"]

    dtypes = {0: "string", 1: "Int64", 2: "float64"}
    with pytest.raises(
        ValueError, match="No regex matching file found in the ZIP archive"
    ):
        process_zip_file(
            mock_zip,
            r"test\.csv$",
            ",",
            1,
            0,
            dtypes,
            3,
            ["NA", ""],
        )

    assert mock_zip.namelist.call_count == 2
    mock_zip.open.assert_not_called()
