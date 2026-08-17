import logging
import os
import sys

if __package__ in (None, ""):  # allow running directly as a script
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List
from flask import Flask, jsonify, request, Response
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


# Pagination bounds. The full English Wikipedia holds millions of articles;
# unbounded responses would serialize the entire result set into one JSON
# payload.
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000


def get_pagination_params():
    """Read and validate limit/offset query parameters.

    Returns (limit, offset, error_response). error_response is None when the
    parameters are valid.
    """
    try:
        limit = int(request.args.get("limit", DEFAULT_PAGE_SIZE))
        offset = int(request.args.get("offset", 0))
    except (TypeError, ValueError):
        return None, None, (jsonify({"error": "limit and offset must be integers"}), 400)
    if limit < 1 or limit > MAX_PAGE_SIZE:
        return None, None, (jsonify({"error": f"limit must be between 1 and {MAX_PAGE_SIZE}"}), 400)
    if offset < 0:
        return None, None, (jsonify({"error": "offset must be >= 0"}), 400)
    return limit, offset, None

@app.route("/", methods=["GET"])
def index() -> str:
    """Root endpoint."""
    return "Welcome to the Wikipedia Analysis API!"

@app.route("/categories", methods=["GET"])
def get_categories() -> Response:
    """Fetches unique category names, paginated via ?limit= and ?offset=.

    Results are ordered by name so pages are stable across requests.
    """
    limit, offset, err = get_pagination_params()
    if err:
        return err
    try:
        with get_db_session() as session:
            query = (
                "MATCH (c:Category) RETURN DISTINCT c.name AS categoryName "
                "ORDER BY categoryName SKIP $offset LIMIT $limit"
            )
            result = session.run(query, offset=offset, limit=limit)
            categories: List[str] = [record["categoryName"] for record in result]
        return jsonify(categories)
    except Exception:
        logger.exception("Error fetching categories")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/category/<category_name>", methods=["GET"])
def get_articles_in_category(category_name: str) -> Response:
    """Fetches titles of articles in a category, paginated via ?limit= and ?offset=."""
    if not category_name or not category_name.strip():
        return jsonify({"error": "category_name is required"}), 400
    limit, offset, err = get_pagination_params()
    if err:
        return err
    try:
        with get_db_session() as session:
            query = """
            MATCH (a:Article)-[:BELONGS_TO]->(c:Category)
            WHERE c.name = $category_name
            RETURN a.title AS articleTitle
            ORDER BY articleTitle SKIP $offset LIMIT $limit
            """
            result = session.run(query, category_name=category_name, offset=offset, limit=limit)
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
