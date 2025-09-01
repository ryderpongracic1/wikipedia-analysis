import pytest
from unittest.mock import Mock, patch
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer
import time
from pathlib import Path
import lxml.etree as ET
import re

# Import functions from wikipedia_analysis
from wikipedia_analysis.database import create_constraints_and_indexes, batch_import_nodes, batch_import_relationships
from wikipedia_analysis.data_processing import clean_title

@pytest.fixture
def mock_neo4j_session():
    """Mock Neo4j session for testing"""
    session = Mock()
    # Configure mock behavior if needed
    return session

@pytest.fixture
def sample_article_data():
    """Sample Wikipedia article data for testing"""
    return {
        'id': 12345,
        'title': 'Test Article',
        'namespace': 0,
        'length': 1000,
        'is_redirect': False,
        'is_new': True,
        'is_minor': False
    }

@pytest.fixture
def sample_category_data():
    """Sample Wikipedia category data for testing"""
    return {
        'id': 101,
        'title': 'Test Category',
        'depth': 1
    }

@pytest.fixture
def mock_config():
    """Mock configuration for Neo4j connection details"""
    config_mock = Mock()
    config_mock.NEO4J_URI = "bolt://localhost:7687"
    config_mock.NEO4J_USER = "neo4j"
    config_mock.NEO4J_PASSWORD = "password"
    return config_mock

@pytest.fixture(scope="session")
def neo4j_container():
    """Starts a Neo4j container for integration tests."""
    with Neo4jContainer("neo4j:4.4") as container:
        container.start()
        # Wait for Neo4j to be ready
        # The testcontainers-python Neo4jContainer should handle waiting for readiness,
        # but adding a small delay can sometimes help with flaky tests.
        time.sleep(5)
        yield container

@pytest.fixture(scope="session")
def neo4j_driver(neo4j_container):
    """Creates a Neo4j driver instance connected to the test container."""
    uri = neo4j_container.get_connection_url()
    driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))
    driver.verify_connectivity()
    yield driver
    driver.close()

@pytest.fixture(scope="session")
def populated_neo4j_db(neo4j_driver):
    """
    Populates the Neo4j test database with sample data and ensures cleanup.
    """
    session = neo4j_driver.session()
    try:
        # 1. Create constraints and indexes
        create_constraints_and_indexes(session)

        # 2. Parse sample_data.xml to extract all nodes and relationships
        xml_file_path = Path(__file__).parent / "fixtures" / "sample_data.xml"
        
        articles_to_import = []
        categories_to_import = []
        links_to_relationships = [] # source_id, target_id
        belongs_to_relationships = [] # article_id, category_id
        redirects_to_relationships = [] # source_id, target_id

        article_title_to_id_map = {}
        category_title_to_id_map = {}

        ns = '{http://www.mediawiki.org/xml/export-0.11/}'

        # First pass: Collect all articles and categories, and their IDs
        # This pass also identifies redirects
        for event, elem in ET.iterparse(xml_file_path, events=('end',)):
            if elem.tag == ns + 'page':
                page_id_elem = elem.find(ns + 'id')
                page_title_elem = elem.find(ns + 'title')
                
                if page_id_elem is not None and page_title_elem is not None:
                    page_id = page_id_elem.text
                    page_title = clean_title(page_title_elem.text)
                    
                    if page_title.startswith("Category:"):
                        categories_to_import.append({'id': page_id, 'name': page_title.replace("Category:", "")})
                        category_title_to_id_map[page_title] = page_id
                    else:
                        is_redirect = elem.find(ns + 'redirect') is not None
                        redirect_title = None
                        if is_redirect:
                            redirect_elem = elem.find(ns + 'redirect')
                            if redirect_elem is not None:
                                redirect_title = clean_title(redirect_elem.get('title'))
                        
                        articles_to_import.append({
                            'id': page_id,
                            'title': page_title,
                            'is_redirect': is_redirect,
                            'redirect_title': redirect_title
                        })
                        article_title_to_id_map[page_title] = page_id
                
                # Clear the element and its ancestors to free memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        # Batch import nodes
        batch_import_nodes(session, "Article", articles_to_import)
        batch_import_nodes(session, "Category", categories_to_import)

        # Second pass: Collect relationships
        for event, elem in ET.iterparse(xml_file_path, events=('end',)):
            if elem.tag == ns + 'page':
                page_id_elem = elem.find(ns + 'id')
                page_title_elem = elem.find(ns + 'title')
                text_elem = elem.find(ns + 'revision/' + ns + 'text')

                if page_id_elem is not None and page_title_elem is not None:
                    page_id = page_id_elem.text
                    page_title = clean_title(page_title_elem.text)

                    # Handle REDIRECTS_TO relationships
                    is_redirect = elem.find(ns + 'redirect') is not None
                    if is_redirect:
                        redirect_elem = elem.find(ns + 'redirect')
                        if redirect_elem is not None:
                            redirect_target_title = clean_title(redirect_elem.get('title'))
                            if page_id in article_title_to_id_map.values() and redirect_target_title in article_title_to_id_map:
                                redirects_to_relationships.append({
                                    'source_article_id': page_id,
                                    'target_article_id': article_title_to_id_map[redirect_target_title]
                                })

                    # Handle LINKS_TO and BELONGS_TO relationships from text
                    if text_elem is not None and text_elem.text:
                        link_pattern = re.compile(r'\[\[([^|\]]+)(?:\|[^\]]+)?\]\]')
                        for match in link_pattern.finditer(text_elem.text):
                            link_title = clean_title(match.group(1))
                            if link_title and link_title != page_title: # Avoid self-links
                                if link_title.startswith("Category:"):
                                    if page_id in article_title_to_id_map.values() and link_title in category_title_to_id_map:
                                        belongs_to_relationships.append({
                                            'article_id': page_id,
                                            'category_id': category_title_to_id_map[link_title]
                                        })
                                elif link_title in article_title_to_id_map:
                                    links_to_relationships.append({
                                        'source_article_id': page_id,
                                        'target_article_id': article_title_to_id_map[link_title]
                                    })
                
                # Clear the element and its ancestors to free memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        # Batch import relationships
        batch_import_relationships(session, "LINKS_TO", "Article", "Article", "source_article_id", "target_article_id", links_to_relationships)
        batch_import_relationships(session, "BELONGS_TO", "Article", "Category", "article_id", "category_id", belongs_to_relationships)
        batch_import_relationships(session, "REDIRECTS_TO", "Article", "Article", "source_article_id", "target_article_id", redirects_to_relationships)

        yield neo4j_driver
    finally:
        # Cleanup: Clear the database
        with neo4j_driver.session() as cleanup_session:
            cleanup_session.run("MATCH (n) DETACH DELETE n")
        session.close()