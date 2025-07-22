import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from main import (
    setup_logging,
    get_schema,
    match_schema,
    sync_data,
    trigger_sync,
    SingletonConfig
)


def test_singleton_config():
    """Test that SingletonConfig maintains singleton behavior"""
    config1 = SingletonConfig()
    config2 = SingletonConfig()
    assert config1 is config2


def test_singleton_config_properties():
    """Test SingletonConfig properties"""
    config = SingletonConfig()
    assert config.project_id == "cru-data-warehouse-elt-prod"
    assert config.dataset_id == "temp_okta"
    assert config.target_dataset_id == "el_okta"


@patch('main.json.load')
@patch('builtins.open')
def test_get_schema_success(mock_open, mock_json_load):
    """Test successful schema retrieval"""
    mock_schema = [{"name": "id", "type": "STRING"}]
    mock_json_load.return_value = mock_schema
    
    result = get_schema("okta_users")
    
    assert result == mock_schema
    mock_open.assert_called_once()


@patch('main.json.load')
@patch('builtins.open')
def test_get_schema_file_not_found(mock_open, mock_json_load):
    """Test schema retrieval when file not found"""
    mock_open.side_effect = FileNotFoundError()
    
    result = get_schema("nonexistent_table")
    
    assert result is None


def test_match_schema():
    """Test schema matching functionality"""
    df = pd.DataFrame({
        'id': ['1', '2', '3'],
        'name': ['Alice', 'Bob', 'Charlie'],
        'extra_column': ['x', 'y', 'z']
    })
    
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "age", "type": "INTEGER"}
    ]
    
    result = match_schema(df, schema)
    
    # Should contain only schema columns
    expected_columns = {'id', 'name', 'age'}
    assert set(result.columns) == expected_columns
    
    # Should not contain extra_column
    assert 'extra_column' not in result.columns


@patch('main.get_general_credentials')
@patch('main.get_request')
@patch('main.get_schema')
@patch('main.upload_dataframe_to_bigquery')
@patch('main.write_to_csv')
def test_sync_data_success(mock_csv, mock_upload, mock_schema, mock_request, mock_creds):
    """Test successful data sync"""
    # Mock the API response
    mock_response = Mock()
    mock_response.json.return_value = [{"id": "1", "name": "Test App"}]
    mock_response.links = {}
    mock_request.return_value = mock_response
    
    # Mock credentials and schema
    mock_creds.return_value = "Bearer test-token"
    mock_schema.return_value = [{"name": "id", "type": "STRING"}, {"name": "name", "type": "STRING"}]
    
    # Run sync_data
    sync_data("apps")
    
    # Verify calls were made
    mock_creds.assert_called_with("OKTA_TOKEN")
    mock_schema.assert_called_with("okta_apps")
    mock_upload.assert_called_once()
    mock_csv.assert_called_once()


@patch('main.setup_logging')
@patch('main.sync_data')
@patch('main.sync_all_users')
@patch('main.replace_dataset_bigquery')
@patch('main.dbt_run')
@patch('main.upload_log')
def test_trigger_sync_success(mock_upload_log, mock_dbt, mock_replace, mock_sync_users, mock_sync_data, mock_setup_logging):
    """Test successful trigger_sync execution"""
    
    trigger_sync()
    
    # Verify all sync operations were called
    assert mock_sync_data.call_count == 3  # apps, users, groups
    assert mock_sync_users.call_count == 2  # group_members, app_users
    mock_replace.assert_called_once()
    mock_dbt.assert_called_once_with("10206", "85521", "DBT_TOKEN")
    mock_upload_log.assert_called_once()


@patch('main.setup_logging')
@patch('main.sync_data')
def test_trigger_sync_failure(mock_sync_data, mock_setup_logging):
    """Test trigger_sync handles exceptions"""
    mock_sync_data.side_effect = Exception("Test error")
    
    with pytest.raises(Exception, match="Test error"):
        trigger_sync()


if __name__ == "__main__":
    pytest.main([__file__])