import logging
from neo4j import GraphDatabase, basic_auth, Transaction, Driver
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Neo4jConnectionManager:
    def __init__(self, uri, username, password):
        self._uri = uri
        self._username = username
        self._password = password
        self._driver: Optional[Driver] = None

    def connect(self):
        if self._driver is None:
            try:
                self._driver = GraphDatabase.driver(self._uri, auth=basic_auth(self._username, self._password))
                self._driver.verify_connectivity()
                logger.info("Neo4j connection established successfully.")
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j: {e}")
                self._driver = None
                raise
        return self._driver

    def close(self):
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed.")

    def get_driver(self) -> Optional[Driver]:
        return self._driver

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def create_article_node(session, article_data: Dict[str, Any]):
    query = """
    CREATE (a:Article {
        id: $id,
        title: $title,
        namespace: $namespace,
        redirect_title: $redirect_title,
        is_redirect: $is_redirect
    })
    RETURN a
    """
    session.run(query, article_data)

def create_category_node(session, category_data: Dict[str, Any]):
    query = """
    CREATE (c:Category {
        id: $id,
        name: $name
    })
    RETURN c
    """
    session.run(query, category_data)

def create_links_to_relationship(session, source_article_id: str, target_article_id: str):
    query = """
    MATCH (a:Article {id: $source_article_id})
    MATCH (b:Article {id: $target_article_id})
    MERGE (a)-[:LINKS_TO]->(b)
    """
    session.run(query, source_article_id=source_article_id, target_article_id=target_article_id)

def create_belongs_to_relationship(session, article_id: str, category_id: str):
    query = """
    MATCH (a:Article {id: $article_id})
    MATCH (c:Category {id: $category_id})
    MERGE (a)-[:BELONGS_TO]->(c)
    """
    session.run(query, article_id=article_id, category_id=category_id)

def create_redirects_to_relationship(session, source_article_id: str, target_article_id: str):
    query = """
    MATCH (a:Article {id: $source_article_id})
    MATCH (b:Article {id: $target_article_id})
    MERGE (a)-[:REDIRECTS_TO]->(b)
    """
    session.run(query, source_article_id=source_article_id, target_article_id=target_article_id)

def create_constraints_and_indexes(session):
    constraints_and_indexes = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Article) REQUIRE a.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.title)",
        "CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.namespace)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name)"
    ]
    for query in constraints_and_indexes:
        session.run(query)

def batch_import_nodes(session, node_label: str, nodes_data: List[Dict[str, Any]]):
    if not nodes_data:
        return

    # Determine properties from the first node, assuming all nodes have the same structure
    # This is a simplification; a more robust solution might handle varying properties
    properties = list(nodes_data[0].keys())
    
    # Construct the Cypher query dynamically
    # Example: UNWIND $nodes AS node CREATE (n:Article {id: node.id, title: node.title})
    set_clauses = ", ".join([f"{prop}: node.{prop}" for prop in properties])
    query = f"""
    UNWIND $nodes AS node
    CREATE (n:{node_label} {{{set_clauses}}})
    """
    session.run(query, nodes=nodes_data)

def batch_import_relationships(session, relationship_type: str, from_label: str, to_label: str, from_id_prop: str, to_id_prop: str, relationships_data: List[Dict[str, Any]]):
    if not relationships_data:
        return

    query = f"""
    UNWIND $relationships AS rel
    MATCH (from_node:{from_label} {{id: rel.{from_id_prop}}})
    MATCH (to_node:{to_label} {{id: rel.{to_id_prop}}})
    MERGE (from_node)-[:{relationship_type}]->(to_node)
    """
    session.run(query, relationships=relationships_data)
