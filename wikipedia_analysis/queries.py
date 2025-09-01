# wikipedia_analysis/queries.py

def build_article_query(title=None, namespace=None, length=None):
    """
    Builds a Cypher query to match or create an Article node.
    """
    match_clauses = []
    if title:
        match_clauses.append(f"title: '{title}'")
    if namespace:
        match_clauses.append(f"namespace: {namespace}")
    if length:
        match_clauses.append(f"length: {length}")

    if match_clauses:
        return f"MATCH (a:Article {{{', '.join(match_clauses)}}}) RETURN a"
    return "MATCH (a:Article) RETURN a"

def build_category_query(name=None):
    """
    Builds a Cypher query to match or create a Category node.
    """
    if name:
        return f"MATCH (c:Category {{name: '{name}'}}) RETURN c"
    return "MATCH (c:Category) RETURN c"

def build_links_to_query(from_title, to_title):
    """
    Builds a Cypher query to create a LINKS_TO relationship between two articles.
    """
    return (
        f"MATCH (from:Article {{title: '{from_title}'}}), "
        f"(to:Article {{title: '{to_title}'}}) "
        f"MERGE (from)-[:LINKS_TO]->(to)"
    )

def build_belongs_to_query(article_title, category_name):
    """
    Builds a Cypher query to create a BELONGS_TO relationship between an article and a category.
    """
    return (
        f"MATCH (a:Article {{title: '{article_title}'}}), "
        f"(c:Category {{name: '{category_name}'}}) "
        f"MERGE (a)-[:BELONGS_TO]->(c)"
    )

def build_redirects_to_query(from_title, to_title):
    """
    Builds a Cypher query to create a REDIRECTS_TO relationship between two articles.
    """
    return (
        f"MATCH (from:Article {{title: '{from_title}'}}), "
        f"(to:Article {{title: '{to_title}'}}) "
        f"MERGE (from)-[:REDIRECTS_TO]->(to)"
    )

def build_pagerank_query(max_iterations=20, damping_factor=0.85):
    """
    Builds a Cypher query to run the PageRank algorithm.
    """
    return (
        f"CALL gds.pageRank.stream('wikiGraph', {{ "
        f"maxIterations: {max_iterations}, dampingFactor: {damping_factor} "
        f"}}) YIELD nodeId, score "
        f"RETURN gds.util.asNode(nodeId).title AS article, score "
        f"ORDER BY score DESC"
    )

def build_shortest_path_query(start_title, end_title, relationship_type='LINKS_TO'):
    """
    Builds a Cypher query to find the shortest path between two articles.
    """
    return (
        f"MATCH (start:Article {{title: '{start_title}'}}), "
        f"(end:Article {{title: '{end_title}'}}) "
        f"CALL gds.shortestPath.dijkstra.stream('wikiGraph', {{ "
        f"sourceNode: gds.util.asNode(start).id, "
        f"targetNode: gds.util.asNode(end).id, "
        f"relationshipWeightProperty: 'weight', "
        f"relationshipType: '{relationship_type}' "
        f"}}) YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path "
        f"RETURN "
        f"gds.util.asNode(sourceNode).title AS source, "
        f"gds.util.asNode(targetNode).title AS target, "
        f"totalCost, "
        f"[nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS nodesInPath, "
        f"costs "
        f"ORDER BY index"
    )

def build_community_detection_query(algorithm='louvain'):
    """
    Builds a Cypher query to run a community detection algorithm.
    """
    if algorithm == 'louvain':
        return (
            f"CALL gds.louvain.stream('wikiGraph') "
            f"YIELD nodeId, communityId "
            f"RETURN gds.util.asNode(nodeId).title AS article, communityId "
            f"ORDER BY communityId, article"
        )
    elif algorithm == 'label_propagation':
        return (
            f"CALL gds.labelPropagation.stream('wikiGraph') "
            f"YIELD nodeId, communityId "
            f"RETURN gds.util.asNode(nodeId).title AS article, communityId "
            f"ORDER BY communityId, article"
        )
    else:
        raise ValueError(f"Unsupported community detection algorithm: {algorithm}")

def build_batch_create_articles_query(article_data):
    """
    Builds a Cypher query to create multiple Article nodes in a batch.
    article_data is a list of dictionaries, e.g.,
    [{'title': 'Article 1', 'namespace': 0, 'length': 100}, ...]
    """
    if not article_data:
        return ""
    
    # Ensure all keys are present for consistency, fill missing with None or default
    # This is a simplified example; in a real scenario, you might validate input more rigorously
    processed_data = []
    for item in article_data:
        processed_item = {
            'title': item.get('title'),
            'namespace': item.get('namespace'),
            'length': item.get('length')
        }
        processed_data.append(processed_item)

    query_parts = [
        "UNWIND $props AS article_props",
        "CREATE (a:Article)",
        "SET a = article_props",
        "RETURN a"
    ]
    return "\n".join(query_parts)

def build_batch_create_links_query(link_data):
    """
    Builds a Cypher query to create multiple LINKS_TO relationships in a batch.
    link_data is a list of dictionaries, e.g.,
    [{'from_title': 'Article A', 'to_title': 'Article B'}, ...]
    """
    if not link_data:
        return ""

    query_parts = [
        "UNWIND $props AS link_props",
        "MATCH (from:Article {title: link_props.from_title})",
        "MATCH (to:Article {title: link_props.to_title})",
        "MERGE (from)-[:LINKS_TO]->(to)"
    ]
    return "\n".join(query_parts)
