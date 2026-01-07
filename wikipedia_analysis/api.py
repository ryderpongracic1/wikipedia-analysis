import os
import logging
from typing import List, Any
from flask import Flask, jsonify, Response
from neo4j import GraphDatabase, Driver, Session

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Initialize driver
try:
    driver: Driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
except Exception as e:
    logger.error(f"Failed to create Neo4j driver: {e}")
    driver = None

def get_db_session() -> Session:
    """Helper to get a Neo4j session."""
    if not driver:
        raise ConnectionError("Neo4j driver is not initialized.")
    return driver.session()

@app.route("/", methods=["GET"])
def index() -> str:
    """Root endpoint."""
    return "Welcome to the Wikipedia Analysis API!"

@app.route("/categories", methods=["GET"])
def get_categories() -> Response:
    """Fetches all unique category names."""
    try:
        with get_db_session() as session:
            query = "MATCH (c:Category) RETURN DISTINCT c.name AS categoryName"
            result = session.run(query)
            categories: List[str] = [record["categoryName"] for record in result]
        return jsonify(categories)
    except Exception as e:
        logger.error(f"Error fetching categories: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/category/<category_name>", methods=["GET"])
def get_articles_in_category(category_name: str) -> Response:
    """Fetches titles of articles belonging to a specific category."""
    try:
        with get_db_session() as session:
            query = """
            MATCH (a:Article)-[:IN_CATEGORY]->(c:Category)
            WHERE c.name = $category_name
            RETURN a.title AS articleTitle
            """
            result = session.run(query, category_name=category_name)
            articles: List[str] = [record["articleTitle"] for record in result]
        return jsonify(articles)
    except Exception as e:
        logger.error(f"Error fetching articles for category '{category_name}': {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
