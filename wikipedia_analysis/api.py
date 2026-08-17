import logging
import os
import sys

if __package__ in (None, ""):  # allow running directly as a script
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List
from flask import Flask, jsonify, Response
from neo4j import GraphDatabase, Driver, Session
from wikipedia_analysis.config import load_neo4j_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

try:
    _cfg = load_neo4j_config()
    driver: Driver = GraphDatabase.driver(_cfg.uri, auth=(_cfg.user, _cfg.password))
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
    except Exception:
        logger.exception("Error fetching categories")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/category/<category_name>", methods=["GET"])
def get_articles_in_category(category_name: str) -> Response:
    """Fetches titles of articles belonging to a specific category."""
    if not category_name or not category_name.strip():
        return jsonify({"error": "category_name is required"}), 400
    try:
        with get_db_session() as session:
            query = """
            MATCH (a:Article)-[:BELONGS_TO]->(c:Category)
            WHERE c.name = $category_name
            RETURN a.title AS articleTitle
            """
            result = session.run(query, category_name=category_name)
            articles: List[str] = [record["articleTitle"] for record in result]
        return jsonify(articles)
    except Exception:
        logger.exception("Error fetching articles for category '%s'", category_name)
        return jsonify({"error": "Internal server error"}), 500

def main() -> None:
    # Debug mode exposes the Werkzeug remote-debugger console; opt-in only.
    debug = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(debug=debug)


if __name__ == "__main__":
    main()
