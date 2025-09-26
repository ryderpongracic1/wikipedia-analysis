"""Configuration management for Wikipedia Analysis project."""
import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class Neo4jConfig:
    """Neo4j database configuration."""
    uri: str
    user: str
    password: str
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        if not self.uri or not isinstance(self.uri, str):
            raise ValueError("URI must be a non-empty string.")
        if not self.user or not isinstance(self.user, str):
            raise ValueError("User must be a non-empty string.")
        if not self.password or not isinstance(self.password, str):
            raise ValueError("Password must be a non-empty string.")


def load_neo4j_config(config_file_path: Optional[str] = None) -> Neo4jConfig:
    """Load Neo4j configuration from environment variables or config file."""
    # Priority: Environment variables > Config file > Defaults
    config = {}
    
    # Try to load from config file first
    if config_file_path and os.path.exists(config_file_path):
        try:
            with open(config_file_path, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)
        except (json.JSONDecodeError, IOError) as e:
            raise ValueError(f"Failed to load config file: {e}")
    
    # Override with environment variables
    neo4j_config = Neo4jConfig(
        uri=os.getenv('NEO4J_URI', config.get('neo4j_uri', 'bolt://localhost:7687')),
        user=os.getenv('NEO4J_USER', config.get('neo4j_user', 'neo4j')),
        password=os.getenv('NEO4J_PASSWORD', config.get('neo4j_password', 'neo4j'))
    )
    
    neo4j_config.validate()
    return neo4j_config


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