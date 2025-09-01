# wikipedia_analysis/analysis.py

import json
import csv
import time

def calculate_pagerank(session, project_name="wikipedia"):
    """
    Calculates PageRank for nodes in the graph.
    """
    query = f"""
    CALL gds.pageRank.stream('{project_name}', {{
        maxIterations: 20,
        dampingFactor: 0.85,
        relationshipWeightProperty: 'weight'
    }})
    YIELD nodeId, score
    RETURN gds.util.asNode(nodeId).title AS title, score
    ORDER BY score DESC
    """
    results = session.run(query)
    return [{"title": r["title"], "score": r["score"]} for r in results]

def find_shortest_path(session, start_node_title, end_node_title, project_name="wikipedia"):
    """
    Finds the shortest path between two nodes using BFS.
    """
    query = f"""
    MATCH (start:Article {{title: $start_node_title}}), (end:Article {{title: $end_node_title}})
    CALL gds.shortestPath.bfs.stream('{project_name}', {{
        sourceNode: gds.util.asNode(start).id,
        targetNode: gds.util.asNode(end).id,
        relationshipWeightProperty: 'weight'
    }})
    YIELD index, sourceNode, targetNode, totalCost, nodeIds, relationshipIds
    RETURN
        [nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS path,
        totalCost AS length
    """
    results = session.run(query, start_node_title=start_node_title, end_node_title=end_node_title)
    return [{"path": r["path"], "length": r["length"]} for r in results]

def detect_communities(session, project_name="wikipedia"):
    """
    Detects communities using the Louvain algorithm.
    """
    query = f"""
    CALL gds.louvain.stream('{project_name}', {{
        relationshipWeightProperty: 'weight'
    }})
    YIELD nodeId, communityId
    RETURN gds.util.asNode(nodeId).title AS title, communityId
    ORDER BY communityId, title
    """
    results = session.run(query)
    communities = {}
    for r in results:
        community_id = r["communityId"]
        if community_id not in communities:
            communities[community_id] = []
        communities[community_id].append(r["title"])
    return communities

def calculate_centrality(session, project_name="wikipedia", centrality_type="betweenness"):
    """
    Calculates various centrality measures.
    """
    if centrality_type == "betweenness":
        query = f"""
        CALL gds.betweenness.stream('{project_name}', {{
            relationshipWeightProperty: 'weight'
        }})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS title, score
        ORDER BY score DESC
        """
    elif centrality_type == "closeness":
        query = f"""
        CALL gds.closeness.stream('{project_name}', {{
            relationshipWeightProperty: 'weight'
        }})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS title, score
        ORDER BY score DESC
        """
    else:
        raise ValueError(f"Unsupported centrality type: {centrality_type}")

    results = session.run(query)
    return [{"title": r["title"], "score": r["score"]} for r in results]

def export_results(data, format_type="json", filename="results"):
    """
    Exports analysis results to a specified format (JSON or CSV).
    """
    if format_type == "json":
        with open(f"{filename}.json", "w") as f:
            json.dump(data, f, indent=4)
    elif format_type == "csv":
        if not data:
            return
        with open(f"{filename}.csv", "w", newline="") as f:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    else:
        raise ValueError(f"Unsupported export format: {format_type}")

def measure_performance(func, *args, **kwargs):
    """
    Measures the execution time of a given function.
    """
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    return result, (end_time - start_time)