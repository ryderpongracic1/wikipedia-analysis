"""Configuration management for Wikipedia Analysis project."""
import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass

# Centralized ConfigError used by tests and callers
class ConfigError(Exception):
    """Raised for configuration loading/validation errors."""
    pass


@dataclass
class Neo4jConfig:
    """Neo4j database configuration."""
    uri: str
    user: str
    password: str

    def __post_init__(self):
        # Backward-compatible uppercase attributes expected in tests
        self.NEO4J_URI = self.uri
        self.NEO4J_USER = self.user
        self.NEO4J_PASSWORD = self.password

    def validate(self) -> None:
        """Validate configuration parameters."""
        if not self.uri or not isinstance(self.uri, str):
            raise ConfigError("URI must be a non-empty string.")
        if not self.user or not isinstance(self.user, str):
            raise ConfigError("User must be a non-empty string.")
        if not self.password or not isinstance(self.password, str):
            raise ConfigError("Password must be a non-empty string.")


def load_neo4j_config_from_file(config_path: str = "config.json") -> Neo4jConfig:
    """
    Load Neo4j config from a JSON file.

    Expected JSON structure:
    {
        "neo4j": {
            "uri": "...",
            "user": "...",
            "password": "..."
        }
    }
    """
    try:
        with open(config_path, 'r') as f:
            data = json.load(f)
        neo = data.get("neo4j", {})
        uri = neo.get("uri")
        user = neo.get("user")
        password = neo.get("password")
        if not all([uri, user, password]):
            raise ConfigError("Missing Neo4j configuration fields in file.")
        cfg = Neo4jConfig(uri=uri, user=user, password=password)
        cfg.validate()
        return cfg
    except FileNotFoundError:
        # Normalize file missing to ConfigError for tests
        raise ConfigError(f"Configuration file not found at {config_path}")
    except json.JSONDecodeError:
        raise ConfigError(f"Error decoding JSON from {config_path}")


def load_neo4j_config(config_file_path: Optional[str] = None) -> Neo4jConfig:
    """
    Load Neo4j configuration. Order:
      1) If config_file_path provided -> load_neo4j_config_from_file (exceptions propagate)
      2) Try default file (silently ignore missing/invalid file)
      3) Fallback to environment variables
    """
    # 1) explicit file path preference
    if config_file_path:
        return load_neo4j_config_from_file(config_file_path)

    # 2) try default file
    file_config = None
    try:
        file_config = load_neo4j_config_from_file()
    except ConfigError:
        file_config = None

    # 3) environment overrides
    uri = os.getenv('NEO4J_URI', file_config.uri if file_config else None)
    user = os.getenv('NEO4J_USER', file_config.user if file_config else None)
    password = os.getenv('NEO4J_PASSWORD', file_config.password if file_config else None)

    if not all([uri, user, password]):
        raise ConfigError("Missing Neo4j configuration. Check config file or environment variables.")

    cfg = Neo4jConfig(uri=uri, user=user, password=password)
    cfg.validate()
    return cfg

# Global configuration instance
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'neo4j')

# Default configurations for different environments
DEFAULT_CONFIGS = {
    'development': {
        'neo4j_uri': 'bolt://localhost:7687',
        'neo4j_user': 'neo4j',
        'neo4j_password': 'neo4j'
    },
    'test': {
        'neo4j_uri': 'bolt://localhost:7687',
        'neo4j_user': 'neo4j',
        'neo4j_password': 'testpassword'
    },
    'production': {
        'neo4j_uri': os.getenv('NEO4J_URI', ''),
        'neo4j_user': os.getenv('NEO4J_USER', ''),
        'neo4j_password': os.getenv('NEO4J_PASSWORD', '')
    }
}


def get_config(environment: str = 'development') -> Dict[str, Any]:
    """Get configuration for specified environment."""
    return DEFAULT_CONFIGS.get(environment, DEFAULT_CONFIGS['development'])