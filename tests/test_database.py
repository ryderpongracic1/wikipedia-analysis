import pytest
from unittest.mock import patch, Mock, MagicMock
from neo4j import GraphDatabase
from wikipedia_analysis.database import Neo4jConnectionManager, create_article_node, create_category_node, \
    create_links_to_relationship, create_belongs_to_relationship, create_redirects_to_relationship, \
    create_constraints_and_indexes, batch_import_nodes, batch_import_relationships

# Test Neo4j connection establishment and teardown
def test_connection_manager_connect_success(mock_config):
    with patch('neo4j.GraphDatabase.driver') as mock_driver:
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        manager = Neo4jConnectionManager(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)
        manager.connect()
        
        mock_driver.assert_called_once_with(mock_config.NEO4J_URI, auth=(mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD))
        mock_driver_instance.verify_connectivity.assert_called_once()
        assert manager.get_driver() is not None

def test_connection_manager_connect_failure(mock_config):
    with patch('neo4j.GraphDatabase.driver') as mock_driver:
        mock_driver_instance = MagicMock()
        mock_driver_instance.verify_connectivity.side_effect = Exception("Connection failed")
        mock_driver.return_value = mock_driver_instance
        
        manager = Neo4jConnectionManager(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)
        with pytest.raises(Exception, match="Connection failed"):
            manager.connect()
        
        mock_driver.assert_called_once()
        mock_driver_instance.verify_connectivity.assert_called_once()
        assert manager.get_driver() is None

def test_connection_manager_close(mock_config):
    with patch('neo4j.GraphDatabase.driver') as mock_driver:
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        manager = Neo4jConnectionManager(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)
        manager.connect()
        manager.close()
        
        mock_driver_instance.close.assert_called_once()
        assert manager.get_driver() is None

def test_connection_manager_context_manager(mock_config):
    with patch('neo4j.GraphDatabase.driver') as mock_driver:
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        with Neo4jConnectionManager(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD) as manager:
            mock_driver.assert_called_once()
            mock_driver_instance.verify_connectivity.assert_called_once()
            assert manager.get_driver() is not None
        
        mock_driver_instance.close.assert_called_once()
        assert manager.get_driver() is None

# Test node creation (Article, Category)
def test_create_article_node(mock_neo4j_session, sample_article_data):
    create_article_node(mock_neo4j_session, sample_article_data)
    mock_neo4j_session.run.assert_called_once_with(
        """
    CREATE (a:Article {
        id: $id,
        title: $title,
        namespace: $namespace,
        redirect_title: $redirect_title,
        is_redirect: $is_redirect
    })
    RETURN a
    """,
        sample_article_data
    )

def test_create_category_node(mock_neo4j_session, sample_category_data):
    create_category_node(mock_neo4j_session, sample_category_data)
    mock_neo4j_session.run.assert_called_once_with(
        """
    CREATE (c:Category {
        id: $id,
        name: $name
    })
    RETURN c
    """,
        sample_category_data
    )

# Test relationship creation (LINKS_TO, BELONGS_TO, REDIRECTS_TO)
def test_create_links_to_relationship(mock_neo4j_session):
    source_id = "article1"
    target_id = "article2"
    create_links_to_relationship(mock_neo4j_session, source_id, target_id)
    mock_neo4j_session.run.assert_called_once_with(
        """
    MATCH (a:Article {id: $source_article_id})
    MATCH (b:Article {id: $target_article_id})
    MERGE (a)-[:LINKS_TO]->(b)
    """,
        source_article_id=source_id, target_article_id=target_id
    )

def test_create_belongs_to_relationship(mock_neo4j_session):
    article_id = "article1"
    category_id = "category1"
    create_belongs_to_relationship(mock_neo4j_session, article_id, category_id)
    mock_neo4j_session.run.assert_called_once_with(
        """
    MATCH (a:Article {id: $article_id})
    MATCH (c:Category {id: $category_id})
    MERGE (a)-[:BELONGS_TO]->(c)
    """,
        article_id=article_id, category_id=category_id
    )

def test_create_redirects_to_relationship(mock_neo4j_session):
    source_id = "article1"
    target_id = "article2"
    create_redirects_to_relationship(mock_neo4j_session, source_id, target_id)
    mock_neo4j_session.run.assert_called_once_with(
        """
    MATCH (a:Article {id: $source_article_id})
    MATCH (b:Article {id: $target_article_id})
    MERGE (a)-[:REDIRECTS_TO]->(b)
    """,
        source_article_id=source_id, target_article_id=target_id
    )

# Test constraint and index creation
def test_create_constraints_and_indexes(mock_neo4j_session):
    create_constraints_and_indexes(mock_neo4j_session)
    expected_calls = [
        ("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Article) REQUIRE a.id IS UNIQUE",),
        ("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE",),
        ("CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.title)",),
        ("CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.namespace)",),
        ("CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name)",)
    ]
    assert mock_neo4j_session.run.call_count == len(expected_calls)
    for call_args, _ in zip(mock_neo4j_session.run.call_args_list, expected_calls):
        assert call_args.args[0] in [q[0] for q in expected_calls]

# Test batch import operations
def test_batch_import_nodes(mock_neo4j_session):
    nodes_data = [
        {"id": "art1", "title": "Article 1", "namespace": "0", "redirect_title": None, "is_redirect": False},
        {"id": "art2", "title": "Article 2", "namespace": "0", "redirect_title": None, "is_redirect": False}
    ]
    batch_import_nodes(mock_neo4j_session, "Article", nodes_data)
    mock_neo4j_session.run.assert_called_once()
    args, kwargs = mock_neo4j_session.run.call_args
    assert "UNWIND $nodes AS node" in args[0]
    assert "CREATE (n:Article {id: node.id, title: node.title, namespace: node.namespace, redirect_title: node.redirect_title, is_redirect: node.is_redirect})" in args[0]
    assert kwargs['nodes'] == nodes_data

def test_batch_import_nodes_empty_list(mock_neo4j_session):
    batch_import_nodes(mock_neo4j_session, "Article", [])
    mock_neo4j_session.run.assert_not_called()

def test_batch_import_relationships(mock_neo4j_session):
    relationships_data = [
        {"from_id_prop": "art1", "to_id_prop": "art2"},
        {"from_id_prop": "art3", "to_id_prop": "art4"}
    ]
    batch_import_relationships(mock_neo4j_session, "LINKS_TO", "Article", "Article", "from_id_prop", "to_id_prop", relationships_data)
    mock_neo4j_session.run.assert_called_once()
    args, kwargs = mock_neo4j_session.run.call_args
    assert "UNWIND $relationships AS rel" in args[0]
    assert "MATCH (from_node:Article {id: rel.from_id_prop})" in args[0]
    assert "MATCH (to_node:Article {id: rel.to_id_prop})" in args[0]
    assert "MERGE (from_node)-[:LINKS_TO]->(to_node)" in args[0]
    assert kwargs['relationships'] == relationships_data

def test_batch_import_relationships_empty_list(mock_neo4j_session):
    batch_import_relationships(mock_neo4j_session, "LINKS_TO", "Article", "Article", "from_id_prop", "to_id_prop", [])
    mock_neo4j_session.run.assert_not_called()

# Test transaction handling and rollback scenarios
def test_transaction_success(mock_config):
    with patch('neo4j.GraphDatabase.driver') as mock_driver:
        mock_driver_instance = MagicMock()
        mock_session = MagicMock()
        mock_transaction = Mock()

        mock_driver.return_value = mock_driver_instance
        mock_driver_instance.session.return_value.__enter__.return_value = mock_session
        mock_session.begin_transaction.return_value.__enter__.return_value = mock_transaction

        manager = Neo4jConnectionManager(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)
        with manager.get_driver().session() as session:
            with session.begin_transaction() as tx:
                tx.run("CREATE (n:Test {name: 'Success'})")
            
            mock_transaction.commit.assert_called_once()
            mock_transaction.rollback.assert_not_called()

def test_transaction_rollback_on_failure(mock_config):
    with patch('neo4j.GraphDatabase.driver') as mock_driver:
        mock_driver_instance = MagicMock()
        mock_session = MagicMock()
        mock_transaction = Mock()

        mock_driver.return_value = mock_driver_instance
        mock_driver_instance.session.return_value.__enter__.return_value = mock_session
        mock_session.begin_transaction.return_value.__enter__.return_value = mock_transaction
        mock_transaction.run.side_effect = Exception("Transaction failed")

        manager = Neo4jConnectionManager(mock_config.NEO4J_URI, mock_config.NEO4J_USER, mock_config.NEO4J_PASSWORD)
        with pytest.raises(Exception, match="Transaction failed"):
            with manager.get_driver().session() as session:
                with session.begin_transaction() as tx:
                    tx.run("CREATE (n:Test {name: 'Failure'})")
        
        mock_transaction.rollback.assert_called_once()
        mock_transaction.commit.assert_not_called()

# Data Integrity Tests (Conceptual, as mocking doesn't fully simulate DB constraints)
# These tests primarily verify that the correct Cypher is sent to enforce integrity.
def test_constraint_violation_handling_article_id(mock_neo4j_session, sample_article_data):
    # This test conceptually verifies the query for unique constraint.
    # Actual Neo4j would raise an error if a duplicate ID is inserted.
    # Here, we just ensure the constraint creation function is called.
    create_constraints_and_indexes(mock_neo4j_session)
    # The actual test for constraint violation would be an integration test
    # where a duplicate node insertion would raise a specific Neo4j error.
    # For unit tests, we rely on the `create_constraints_and_indexes` test.
    assert any("REQUIRE a.id IS UNIQUE" in call.args[0] for call in mock_neo4j_session.run.call_args_list)

def test_constraint_violation_handling_category_id(mock_neo4j_session, sample_category_data):
    create_constraints_and_indexes(mock_neo4j_session)
    assert any("REQUIRE c.id IS UNIQUE" in call.args[0] for call in mock_neo4j_session.run.call_args_list)
