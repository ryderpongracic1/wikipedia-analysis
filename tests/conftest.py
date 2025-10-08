import pytest
from unittest.mock import Mock, patch
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer
import time
from pathlib import Path
import lxml.etree as ET
import re
import tempfile
import os

# Import functions from wikipedia_analysis
from wikipedia_analysis.database import create_constraints_and_indexes, batch_import_nodes, batch_import_relationships
from wikipedia_analysis.data_processing import clean_title
from wikipedia_analysis.config import Neo4jConfig

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
        'id': '12345',
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
        'id': '101',
        'title': 'Test Category',
        'depth': 1
    }

@pytest.fixture
def mock_config():
    """Mock configuration for Neo4j connection details"""
    return Neo4jConfig(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="testpassword"
    )

@pytest.fixture
def dummy_xml_file(tmp_path):
    """Create temporary XML file for testing - fixes missing dummy.xml error."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/" xml:lang="en">
    <page>
        <id>1</id>
        <title>Test Article 1</title>
        <ns>0</ns>
        <revision>
            <id>1001</id>
            <text xml:space="preserve">This is a test article with [[Test Article 2]] and [[Category:Test Category]].</text>
        </revision>
    </page>
    <page>
        <id>2</id>
        <title>Test Article 2</title>
        <ns>0</ns>
        <revision>
            <id>1002</id>
            <text xml:space="preserve">This is another test article linking to [[Test Article 1]].</text>
        </revision>
    </page>
    <page>
        <id>101</id>
        <title>Category:Test Category</title>
        <ns>14</ns>
        <revision>
            <id>2001</id>
            <text xml:space="preserve">This is a test category.</text>
        </revision>
    </page>
</mediawiki>"""
    
    # Create both dummy.xml and test_data.xml for different test cases
    dummy_file = tmp_path / "dummy.xml"
    dummy_file.write_text(xml_content, encoding='utf-8')
    
    # Also create it in current directory for tests that expect it there
    try:
        with open("dummy.xml", "w", encoding='utf-8') as f:
            f.write(xml_content)
    except PermissionError:
        pass  # Skip if we can't write to current directory
    
    return str(dummy_file)

@pytest.fixture(scope="session")
def neo4j_container():
    """Starts a Neo4j container for integration tests with proper authentication."""
    # Skip integration container startup if Docker is not available on the host.
    docker_socket = "/var/run/docker.sock"
    if not (os.environ.get("DOCKER_HOST") or os.path.exists(docker_socket)):
        pytest.skip("Docker not available; skipping Neo4j integration tests.")

    container = Neo4jContainer("neo4j:4.4") \
        .with_env("NEO4J_AUTH", "neo4j/testpassword") \
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes") \
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "gds.*,apoc.*") \
        .with_env("NEO4J_dbms_security_procedures_allowlist", "gds.*,apoc.*") \
        .with_exposed_ports(7687, 7474)
    
    container.start()
    
    # Wait for Neo4j to be fully ready with retry logic
    max_wait_time = 60  # seconds
    wait_interval = 2   # seconds
    waited = 0
    
    while waited < max_wait_time:
        try:
            # Test connection
            uri = f"bolt://localhost:{container.get_exposed_port(7687)}"
            test_driver = GraphDatabase.driver(uri, auth=("neo4j", "testpassword"))
            test_driver.verify_connectivity()
            test_driver.close()
            break
        except Exception:
            time.sleep(wait_interval)
            waited += wait_interval
    
    if waited >= max_wait_time:
        container.stop()
        raise RuntimeError("Neo4j container failed to start within timeout")
    
    yield container
    container.stop()

@pytest.fixture(scope="session")
def neo4j_driver(neo4j_container):
    """Creates a Neo4j driver instance connected to the test container."""
    uri = f"bolt://localhost:{neo4j_container.get_exposed_port(7687)}"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "testpassword"))
    
    # Retry connection with exponential backoff
    max_retries = 5
    for attempt in range(max_retries):
        try:
            driver.verify_connectivity()
            break
        except Exception as e:
            if attempt == max_retries - 1:
                driver.close()
                raise RuntimeError(f"Failed to connect to Neo4j after {max_retries} attempts: {e}")
            time.sleep(2 ** attempt)
    
    yield driver
    driver.close()

@pytest.fixture(scope="session")
def populated_neo4j_db(neo4j_driver):
    """
    Populates the Neo4j test database with sample data and ensures cleanup.
    Uses context-managed sessions to ensure sessions are closed cleanly.
    """
    try:
        with neo4j_driver.session() as session:
            # 1. Create constraints and indexes
            create_constraints_and_indexes(session)

            # 2. Create sample data directly (not from XML parsing)
            # Create sample articles
            articles_to_import = [
                {'id': '1', 'title': 'Test Article 1'},
                {'id': '2', 'title': 'Test Article 2'},
                {'id': '3', 'title': 'Test Article 3'}
            ]
            
            # Create sample categories
            categories_to_import = [
                {'id': '101', 'name': 'Test Category'}
            ]
            
            # Batch import nodes
            batch_import_nodes(session, "Article", articles_to_import)
            batch_import_nodes(session, "Category", categories_to_import)

            # Create sample relationships
            links_relationships = [
                {'source_article_id': '1', 'target_article_id': '2'},
                {'source_article_id': '2', 'target_article_id': '3'}
            ]
            
            belongs_to_relationships = [
                {'article_id': '1', 'category_id': '101'}
            ]
            
            # Batch import relationships
            batch_import_relationships(
                session,
                "LINKS_TO", "Article", "Article",
                "source_article_id", "target_article_id",
                links_relationships
            )
            batch_import_relationships(
                session,
                "BELONGS_TO", "Article", "Category",
                "article_id", "category_id",
                belongs_to_relationships
            )

        yield neo4j_driver
    finally:
        # Cleanup: Clear the database using a context-managed session
        with neo4j_driver.session() as cleanup_session:
            cleanup_session.run("MATCH (n) DETACH DELETE n")

@pytest.fixture
def sample_xml_content():
    """Sample XML content for testing data processing."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/" xml:lang="en">
    <page>
        <id>1</id>
        <title>Sample Article</title>
        <ns>0</ns>
        <revision>
            <id>1001</id>
            <text xml:space="preserve">This is a sample article with [[Another Article]] link.</text>
        </revision>
    </page>
</mediawiki>"""

@pytest.fixture
def temp_xml_file(tmp_path, sample_xml_content):
    """Create temporary XML file with sample content."""
    xml_file = tmp_path / "test_data.xml"
    xml_file.write_text(sample_xml_content, encoding='utf-8')
    return str(xml_file)