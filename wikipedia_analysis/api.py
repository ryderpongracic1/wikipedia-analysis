import os
from flask import Flask, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def get_db():
    return driver.session()

@app.route("/categories", methods=["GET"])
def get_categories():
    with get_db() as session:
        query = "MATCH (c:Category) RETURN DISTINCT c.name AS categoryName"
        result = session.run(query)
        categories = [record["categoryName"] for record in result]
    return jsonify(categories)

@app.route("/category/<category_name>", methods=["GET"])
def get_articles_in_category(category_name):
    with get_db() as session:
        query = """
        MATCH (a:Article)-[:IN_CATEGORY]->(c:Category)
        WHERE c.name = $category_name
        RETURN a.title AS articleTitle
        """
        result = session.run(query, category_name=category_name)
        articles = [record["articleTitle"] for record in result]
    return jsonify(articles)

@app.route("/")
def index():
    return "Welcome to the Wikipedia Analysis API!"

if __name__ == "__main__":
    app.run(debug=True)