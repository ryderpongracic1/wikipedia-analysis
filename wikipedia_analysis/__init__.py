"""Wikipedia Analysis Package

A Python package for importing Wikipedia data into Neo4j and performing
network analysis on the resulting graph.
"""

__version__ = "1.0.0"
__author__ = "Ryder Pongracic"
__email__ = "ryderjpm@gmail.com"

# Import configuration first
from .config import load_neo4j_config, Neo4jConfig, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# Import database functionality with proper aliases
from .database import (
    Neo4jConnectionManager as ConnectionManager,
    create_constraints_and_indexes, 
    batch_import_nodes, 
    batch_import_relationships,
    create_article_node,
    create_category_node
)

# Import data processing functions that actually exist
from .data_processing import (
    clean_title, 
    parse_dump_file, 
    batch_data,
    validate_length,
    transform_to_article_node,
    transform_to_category_node
)

# Import query building functions
from .queries import (
    build_article_query,
    build_category_query,
    build_links_to_query,
    build_belongs_to_query,
    build_redirects_to_query,
    build_pagerank_query,
    build_shortest_path_query,
    build_community_detection_query
)

# Import analysis functions with error handling for optional GDS
try:
    from .analysis import (
        calculate_pagerank,
        find_shortest_path,
        detect_communities,
        calculate_centrality,
        export_results,
        measure_performance,
        gds  # Include gds for backward compatibility
    )
    ANALYSIS_AVAILABLE = True
except ImportError as e:
    ANALYSIS_AVAILABLE = False
    # Provide mock functions if analysis module has issues
    def calculate_pagerank(*args, **kwargs):
        return []
    def find_shortest_path(*args, **kwargs):
        return []
    def detect_communities(*args, **kwargs):
        return {}
    def calculate_centrality(*args, **kwargs):
        return []
    def export_results(*args, **kwargs):
        pass
    def measure_performance(func, *args, **kwargs):
        return func(*args, **kwargs), 0.0
    
    class MockGDS:
        class util:
            @staticmethod
            def asNode(data):
                return data
    gds = MockGDS()

__all__ = [
    # Configuration
    'load_neo4j_config',
    'Neo4jConfig',
    'NEO4J_URI',
    'NEO4J_USER', 
    'NEO4J_PASSWORD',
    # Database functions
    'ConnectionManager',
    'create_constraints_and_indexes',
    'batch_import_nodes', 
    'batch_import_relationships',
    'create_article_node',
    'create_category_node',
    # Data processing functions
    'clean_title',
    'parse_dump_file',
    'batch_data',
    'validate_length',
    'transform_to_article_node',
    'transform_to_category_node',
    # Query building functions
    'build_article_query',
    'build_category_query',
    'build_links_to_query',
    'build_belongs_to_query',
    'build_redirects_to_query',
    'build_pagerank_query',
    'build_shortest_path_query',
    'build_community_detection_query',
    # Analysis functions
    'calculate_pagerank',
    'find_shortest_path',
    'detect_communities',
    'calculate_centrality',
    'export_results',
    'measure_performance',
    'gds',
    'ANALYSIS_AVAILABLE'
]
