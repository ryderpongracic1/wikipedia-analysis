# wikipedia_analysis/queries.py

def build_article_query(title=None, namespace=None, length=None):
    """
    Builds a parameterized Cypher query to match Article nodes.
    Returns (query_string, params_dict).
    """
    params = {}
    match_clauses = []
    if title:
        match_clauses.append("title: $title")
        params["title"] = title
    if namespace is not None:
        match_clauses.append("namespace: $namespace")
        params["namespace"] = namespace
    if length is not None:
        match_clauses.append("length: $length")
        params["length"] = length
    if match_clauses:
        return f"MATCH (a:Article {{{', '.join(match_clauses)}}}) RETURN a", params
    return "MATCH (a:Article) RETURN a", {}

def build_category_query(name=None):
    """
    Builds a parameterized Cypher query to match Category nodes.
    Returns (query_string, params_dict).
    """
    if name:
        return "MATCH (c:Category {name: $name}) RETURN c", {"name": name}
    return "MATCH (c:Category) RETURN c", {}

def build_links_to_query(from_title, to_title):
    """
    Builds a parameterized Cypher query to create a LINKS_TO relationship.
    Returns (query_string, params_dict).
    """
    query = (
        "MATCH (from:Article {title: $from_title}), "
        "(to:Article {title: $to_title}) "
        "MERGE (from)-[:LINKS_TO]->(to)"
    )
    return query, {"from_title": from_title, "to_title": to_title}

def build_belongs_to_query(article_title, category_name):
    """
    Builds a parameterized Cypher query to create a BELONGS_TO relationship.
    Returns (query_string, params_dict).
    """
    query = (
        "MATCH (a:Article {title: $article_title}), "
        "(c:Category {name: $category_name}) "
        "MERGE (a)-[:BELONGS_TO]->(c)"
    )
    return query, {"article_title": article_title, "category_name": category_name}

def build_redirects_to_query(from_title, to_title):
    """
    Builds a parameterized Cypher query to create a REDIRECTS_TO relationship.
    Returns (query_string, params_dict).
    """
    query = (
        "MATCH (from:Article {title: $from_title}), "
        "(to:Article {title: $to_title}) "
        "MERGE (from)-[:REDIRECTS_TO]->(to)"
    )
    return query, {"from_title": from_title, "to_title": to_title}

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
    Builds a parameterized Cypher query for shortest path between two articles.
    Returns (query_string, params_dict). relationship_type is not parameterizable
    in GDS config so remains interpolated.
    """
    query = (
        "MATCH (start:Article {title: $start_title}), "
        "(end:Article {title: $end_title}) "
        "CALL gds.shortestPath.dijkstra.stream('wikiGraph', { "
        "sourceNode: gds.util.asNode(start).id, "
        "targetNode: gds.util.asNode(end).id, "
        "relationshipWeightProperty: 'weight', "
        f"relationshipType: '{relationship_type}' "
        "}) YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path "
        "RETURN "
        "gds.util.asNode(sourceNode).title AS source, "
        "gds.util.asNode(targetNode).title AS target, "
        "totalCost, "
        "[nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS nodesInPath, "
        "costs "
        "ORDER BY index"
    )
    return query, {"start_title": start_title, "end_title": end_title}

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
