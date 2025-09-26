"""Wikipedia Analysis Package

A Python package for importing Wikipedia data into Neo4j and performing
network analysis on the resulting graph.
"""

__version__ = "1.0.0"
__author__ = "Ryder Pongracic"
__email__ = "ryderjpm@gmail.com"

# Import main functionality for easy access
from .database import create_constraints_and_indexes, batch_import_nodes, batch_import_relationships
from .data_processing import clean_title, extract_wikipedia_links, parse_page
from .queries import get_most_cited_articles, get_shortest_path
from .analysis import calculate_pagerank, get_influential_articles

__all__ = [
    'create_constraints_and_indexes',
    'batch_import_nodes', 
    'batch_import_relationships',
    'clean_title',
    'extract_wikipedia_links',
    'parse_page',
    'get_most_cited_articles',
    'get_shortest_path',
    'calculate_pagerank',
    'get_influential_articles'
]
