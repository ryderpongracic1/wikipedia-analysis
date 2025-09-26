"""Wikipedia Analysis Package

A Python package for importing Wikipedia data into Neo4j and performing
network analysis on the resulting graph.
"""

__version__ = "1.0.0"
__author__ = "Ryder Pongracic"
__email__ = "ryderjpm@gmail.com"

# Import main functionality for easy access
from .database import create_constraints_and_indexes, batch_import_nodes, batch_import_relationships
from .data_processing import clean_title, parse_dump_file, batch_data
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
from .analysis import (
    calculate_pagerank,
    find_shortest_path,
    detect_communities,
    calculate_centrality,
    export_results,
    measure_performance
)

__all__ = [
    # Database functions
    'create_constraints_and_indexes',
    'batch_import_nodes', 
    'batch_import_relationships',
    # Data processing functions
    'clean_title',
    'parse_dump_file',
    'batch_data',
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
    'measure_performance'
]
