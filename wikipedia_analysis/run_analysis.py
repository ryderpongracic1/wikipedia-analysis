from neo4j import GraphDatabase

# --- Configuration ---
# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
# IMPORTANT: Replace "your_password" with your actual Neo4j password
password = "my_password"

# --- Analysis Functions ---

def find_most_authoritative_articles(session, limit=20):
    """
    Identifies authoritative articles by counting incoming links (in-degree).
    These are the most heavily referenced articles in the network.
    """
    print(f"\n--- Finding Top {limit} Most Authoritative Articles (by incoming links) ---")
    query = """
    MATCH (a:Article)
    // Calculate the in-degree by counting incoming :LINKS_TO relationships
    WITH a, size((a)<-[:LINKS_TO]-()) as in_degree
    // Return the title and the score
    RETURN a.title AS article, in_degree
    ORDER BY in_degree DESC
    LIMIT $limit
    """
    result = session.run(query, limit=limit)
    for i, record in enumerate(result, 1):
        print(f"{i}. {record['article']} (Cited by {record['in_degree']} articles)")

def calculate_influence_score(session):
    """
    Builds a Wikipedia Influence Score using the PageRank algorithm.
    PageRank measures influence by considering both the quantity and quality of incoming links.
    
    NOTE: This requires the Neo4j Graph Data Science (GDS) library to be installed.
    See: https://neo4j.com/docs/graph-data-science/current/installation/
    """
    print("\n--- Calculating Influence Score for all Articles (using PageRank) ---")
    print("This may take a few minutes on the full dataset...")

    # 1. Check if the GDS graph projection already exists, and drop it if it does.
    check_graph_query = "CALL gds.graph.exists('wikipedia_graph') YIELD exists"
    result = session.run(check_graph_query)
    if result.single()['exists']:
        print("Dropping existing GDS graph projection...")
        drop_graph_query = "CALL gds.graph.drop('wikipedia_graph')"
        session.run(drop_graph_query)
    
    # 2. Project the graph into GDS's in-memory format for high-speed analysis.
    print("Projecting graph into GDS memory...")
    project_graph_query = """
    CALL gds.graph.project(
        'wikipedia_graph', 
        'Article', 
        'LINKS_TO'
    )
    """
    session.run(project_graph_query)
    
    # 3. Run the PageRank algorithm and write the results back to the nodes.
    print("Running PageRank algorithm...")
    pagerank_query = """
    CALL gds.pageRank.write(
        'wikipedia_graph', 
        { writeProperty: 'influence_score' }
    )
    """
    session.run(pagerank_query)
    print("PageRank calculation complete. 'influence_score' property added to nodes.")

def find_top_influencers(session, limit=20):
    """
    Finds the articles with the highest influence score after PageRank has been run.
    """
    print(f"\n--- Finding Top {limit} Most Influential Articles (by PageRank score) ---")
    query = """
    MATCH (a:Article)
    WHERE a.influence_score IS NOT NULL
    RETURN a.title AS article, a.influence_score AS score
    ORDER BY score DESC
    LIMIT $limit
    """
    result = session.run(query, limit=limit)
    for i, record in enumerate(result, 1):
        print(f"{i}. {record['article']} (Influence Score: {record['score']:.4f})")
        
def find_knowledge_path(session, start_article, end_article):
    """
    Tracks the flow of knowledge by finding the shortest path of links
    between two articles.
    """
    print(f"\n--- Finding shortest knowledge path from '{start_article}' to '{end_article}' ---")
    query = """
    MATCH (start:Article {title: $start_article}), (end:Article {title: $end_article})
    // Use the shortestPath function to find a path up to 10 links deep.
    MATCH path = shortestPath((start)-[:LINKS_TO*..10]->(end))
    // Return the titles of the articles in the path.
    RETURN [node in nodes(path) | node.title] AS path_titles
    """
    result = session.run(query, start_article=start_article, end_article=end_article)
    path_record = result.single()
    if path_record:
        print(" -> ".join(path_record['path_titles']))
    else:
        print(f"No path found (within 10 steps) between '{start_article}' and '{end_article}'.")

# --- Main Execution ---
if __name__ == "__main__":
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            # Run the analyses
            find_most_authoritative_articles(session)
            
            # Note: The PageRank functions require Neo4j GDS library.
            # If you don't have it, you can comment out the next two function calls.
            calculate_influence_score(session)
            find_top_influencers(session)

            # Find a path between two interesting topics
            find_knowledge_path(session, "Graph theory", "Social network")
            find_knowledge_path(session, "United States", "World War II")
