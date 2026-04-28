import pytest
from unittest.mock import patch, mock_open, MagicMock
import os
import json
from neo4j import GraphDatabase

# Assuming the config module is at wikipedia_analysis/config.py
# We will mock this module for testing purposes.
# For now, let's define a dummy config module structure for the test to pass.
# In a real scenario, you would import the actual config module.

from wikipedia_analysis import config as project_config
from wikipedia_analysis.config import Neo4jConfig
ConfigError = project_config.ConfigError


# --- Tests start here ---

@patch('wikipedia_analysis.config.json.load')
@patch('wikipedia_analysis.config.open', new_callable=mock_open)
def test_load_neo4j_config_from_file_success(mock_file_open, mock_json_load):
    mock_json_load.return_value = {
        "neo4j": {
            "uri": "bolt://localhost:7687",
            "user": "neo4j",
            "password": "test_password"
        }
    }
    
    # Call the project's function so that the patched open/json.load are used
    from wikipedia_analysis import config as project_config
    config = project_config.load_neo4j_config_from_file("config.json")
    assert config.uri == "bolt://localhost:7687"
    assert config.user == "neo4j"
    assert config.password == "test_password"
    mock_file_open.assert_called_once_with("config.json", 'r')
    mock_json_load.assert_called_once()

@patch('wikipedia_analysis.config.json.load')
@patch('wikipedia_analysis.config.open', new_callable=mock_open)
def test_load_neo4j_config_from_file_missing_fields(mock_file_open, mock_json_load):
    mock_json_load.return_value = {
        "neo4j": {
            "uri": "bolt://localhost:7687",
            "user": "neo4j"
            # password is missing
        }
    }
    from wikipedia_analysis import config as project_config
    with pytest.raises(ConfigError, match="Missing Neo4j configuration fields in file."):
        project_config.load_neo4j_config_from_file("config.json")

@patch('wikipedia_analysis.config.open', new_callable=mock_open)
def test_load_neo4j_config_from_file_not_found(mock_file_open):
    mock_file_open.side_effect = FileNotFoundError
    from wikipedia_analysis import config as project_config
    with pytest.raises(ConfigError, match="Configuration file not found at config.json"):
        project_config.load_neo4j_config_from_file("config.json")

@patch('wikipedia_analysis.config.json.load')
@patch('wikipedia_analysis.config.open', new_callable=mock_open)
def test_load_neo4j_config_from_file_json_decode_error(mock_file_open, mock_json_load):
    mock_json_load.side_effect = json.JSONDecodeError("Expecting value", "config.json", 0)
    from wikipedia_analysis import config as project_config
    with pytest.raises(ConfigError, match="Error decoding JSON from config.json"):
        project_config.load_neo4j_config_from_file("config.json")

# Test environment variable handling
@patch('wikipedia_analysis.config.os.getenv')
@patch('wikipedia_analysis.config.load_neo4j_config_from_file')
def test_load_neo4j_config_env_vars_override_file(mock_load_from_file, mock_getenv):
    # Mock file config
    mock_file_config_instance = MagicMock()
    mock_file_config_instance.uri = "bolt://file:7687"
    mock_file_config_instance.user = "file_user"
    mock_file_config_instance.password = "file_password"
    mock_load_from_file.return_value = mock_file_config_instance

    # Mock environment variables
    mock_getenv.side_effect = lambda key, default: {
        'NEO4J_URI': 'bolt://env:7687',
        'NEO4J_USER': 'env_user',
        'NEO4J_PASSWORD': 'env_password'
    }.get(key, default)
 
    from wikipedia_analysis import config as project_config
    config = project_config.load_neo4j_config()
    assert config.uri == "bolt://env:7687"
    assert config.user == "env_user"
    assert config.password == "env_password"
    mock_load_from_file.assert_called_once()
    mock_getenv.assert_any_call('NEO4J_URI', 'bolt://file:7687')
    mock_getenv.assert_any_call('NEO4J_USER', 'file_user')
    mock_getenv.assert_any_call('NEO4J_PASSWORD', 'file_password')

@patch('wikipedia_analysis.config.os.getenv')
@patch('wikipedia_analysis.config.load_neo4j_config_from_file')
def test_load_neo4j_config_only_env_vars(mock_load_from_file, mock_getenv):
    mock_load_from_file.side_effect = ConfigError("File not found") # Simulate no config file

    mock_getenv.side_effect = lambda key, default: {
        'NEO4J_URI': 'bolt://env_only:7687',
        'NEO4J_USER': 'env_only_user',
        'NEO4J_PASSWORD': 'env_only_password'
    }.get(key, default)
 
    from wikipedia_analysis import config as project_config
    config = project_config.load_neo4j_config()
    assert config.uri == "bolt://env_only:7687"
    assert config.user == "env_only_user"
    assert config.password == "env_only_password"
    mock_load_from_file.assert_called_once()
    mock_getenv.assert_any_call('NEO4J_URI', None)
    mock_getenv.assert_any_call('NEO4J_USER', None)
    mock_getenv.assert_any_call('NEO4J_PASSWORD', None)

@patch('wikipedia_analysis.config.os.getenv')
@patch('wikipedia_analysis.config.load_neo4j_config_from_file')
def test_load_neo4j_config_missing_env_vars_and_file(mock_load_from_file, mock_getenv):
    mock_load_from_file.side_effect = ConfigError("File not found")
    mock_getenv.return_value = None # No environment variables set
 
    from wikipedia_analysis import config as project_config
    with pytest.raises(ConfigError, match="Missing Neo4j configuration. Check config file or environment variables."):
        project_config.load_neo4j_config()

# Test configuration validation (required fields, data types)
# These are implicitly tested by the missing fields test for file loading.
# For data types, we assume the underlying json.load handles basic type parsing,
# and our Neo4jConfig constructor expects strings.

def test_neo4j_config_validation_success():
    config = Neo4jConfig("bolt://localhost:7687", "neo4j", "password")
    config.validate()  # Should not raise
    assert config.uri == "bolt://localhost:7687"
    assert config.user == "neo4j"
    assert config.password == "password"

@pytest.mark.parametrize("uri, user, password, expected_error_match", [
    ("", "user", "pass", "URI must be a non-empty string."),
    ("uri", "", "pass", "User must be a non-empty string."),
    ("uri", "user", "", "Password must be a non-empty string."),
    (None, "user", "pass", "URI must be a non-empty string."),
    ("uri", None, "pass", "User must be a non-empty string."),
    ("uri", "user", None, "Password must be a non-empty string."),
    (123, "user", "pass", "URI must be a non-empty string."),
])
def test_neo4j_config_validation_failure(uri, user, password, expected_error_match):
    config = Neo4jConfig(uri=uri, user=user, password=password)
    with pytest.raises(ConfigError, match=expected_error_match):
        config.validate()

# Test mocking database connection attempts
@patch('neo4j.GraphDatabase.driver')
def test_neo4j_connection_successful(mock_neo4j_driver, mock_config):
    # mock_config is from conftest.py
    # Configure the mock driver and session
    mock_driver_instance = MagicMock()
    mock_neo4j_driver.return_value = mock_driver_instance
    mock_driver_instance.verify_connectivity.return_value = None # No exception means success

    # Assuming a function in our config module would use this driver
    # Let's create a dummy function to test this
    def connect_to_neo4j(uri, user, password):
        driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
        finally:
            driver.close()

    # Use the mocked config values
    result = connect_to_neo4j(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)

    assert result is True
    mock_neo4j_driver.assert_called_once_with(mock_config.NEO4J_URI, auth=(mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD))
    mock_driver_instance.verify_connectivity.assert_called_once()
    mock_driver_instance.close.assert_called_once()

@patch('neo4j.GraphDatabase.driver')
def test_neo4j_connection_failed(mock_neo4j_driver, mock_config):
    mock_driver_instance = MagicMock()
    mock_neo4j_driver.return_value = mock_driver_instance
    mock_driver_instance.verify_connectivity.side_effect = Exception("Failed to connect to Neo4j")

    def connect_to_neo4j(uri, user, password):
        driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
        finally:
            driver.close()

    result = connect_to_neo4j(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)

    assert result is False
    mock_neo4j_driver.assert_called_once_with(mock_config.NEO4J_URI, auth=(mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD))
    mock_driver_instance.verify_connectivity.assert_called_once()
    mock_driver_instance.close.assert_called_once()