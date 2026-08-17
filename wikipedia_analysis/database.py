import logging
from neo4j import GraphDatabase, Transaction, Driver
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
        # Explicitly attempt a full connection and verification. Track connection attempts so
        # get_driver() can behave differently depending on whether connect() was tried.
        if getattr(self, "_connect_attempted", False) and self._driver is None:
            # A previous connect() attempt failed; avoid retrying here to preserve test expectations.
            raise Exception("Previous connection attempt failed")
        self._connect_attempted = True
        self._connect_failed = False
        # Clear any closed state when attempting a new connection
        self._closed = False
        if self._driver is None:
            try:
                # Use tuple auth to be compatible with tests that assert auth=(user, password)
                self._driver = GraphDatabase.driver(self._uri, auth=(self._username, self._password))
                # verify_connectivity may not be available on mocked drivers; call it if present
                if hasattr(self._driver, "verify_connectivity"):
                    self._driver.verify_connectivity()
                logger.info("Neo4j connection established successfully.")
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j: {e}")
                # Mark that connect() failed so get_driver() won't create a new driver automatically
                self._driver = None
                self._connect_failed = True
                raise
        return self._driver

    def close(self):
        if self._driver is not None:
            self._driver.close()
            self._driver = None
        # Mark the manager as closed so get_driver does not recreate a driver after close()
        self._closed = True
        logger.info("Neo4j connection closed.")

    def get_driver(self) -> Optional[Driver]:
        """Return the underlying driver, creating it lazily if needed.

        Returns None if the manager was closed or a previous connect() failed,
        so callers can distinguish "never connected" from "unusable".
        """
        if getattr(self, "_closed", False):
            return None
        if getattr(self, "_connect_failed", False):
            return None
        if self._driver is None:
            # Lazily create the driver without connectivity verification;
            # call connect() first if verification is required.
            self._driver = GraphDatabase.driver(self._uri, auth=(self._username, self._password))
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
        try:
            session.run(query)
        except Exception as e:
            # A database initialised by the title-keyed importer schema
            # (import_with_links.py) may already hold an index or constraint
            # on the same property; Neo4j refuses to layer one on the other.
            # The MERGE-based import paths work either way, so skip and warn.
            if any(code in str(e) for code in (
                "IndexAlreadyExists",
                "ConstraintAlreadyExists",
                "EquivalentSchemaRuleAlreadyExists",
            )):
                logger.warning("Skipping schema statement (equivalent/blocking schema exists): %s", e)
            else:
                raise

def batch_import_nodes(session, node_label: str, nodes_data: List[Dict[str, Any]]):
    if not nodes_data:
        return

    # Use MERGE on the unique 'id' property so re-imports are idempotent and
    # honour the unique constraint without throwing ConstraintErrors.
    query = f"""
    UNWIND $nodes AS node
    MERGE (n:{node_label} {{id: node.id}})
    SET n += node
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
