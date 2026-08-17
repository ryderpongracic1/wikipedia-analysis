# wikipedia_analysis/analysis.py

import json
import csv
import logging
import time
from typing import Dict, List, Any, Optional, Callable, Tuple, Union


# ==========================================
# GDS availability
# ==========================================
# All analysis queries are Cypher strings executed server-side; the Python
# `graphdatascience` client is not required. GDS_AVAILABLE only reports
# whether the optional client library is importable.
try:
    from graphdatascience import GraphDataScience  # noqa: F401
    GDS_AVAILABLE = True
except ImportError:
    GDS_AVAILABLE = False

logger = logging.getLogger(__name__)


class _GdsShim:
    """Placeholder retained for backward compatibility.

    Historic versions exported a module-level ``gds`` object that some
    callers and tests monkeypatch. Nothing in this module calls it: every
    ``gds.*`` reference in the queries below is a server-side Cypher
    function, not this Python object.
    """

    class util:
        @staticmethod
        def asNode(node_data: Any) -> Union[Dict[str, Any], Any]:
            if hasattr(node_data, 'get'):
                return node_data
            return {'title': str(node_data), 'id': node_data}


gds = _GdsShim()


# ==========================================
# Analysis Functions
# ==========================================

def calculate_pagerank(session: Any, project_name: str = "wikipedia") -> List[Dict[str, Any]]:
    """
    Calculates PageRank for nodes in the graph.
    Falls back to a Cypher implementation if GDS fails.
    """
    try:
        # NOTE: no relationshipWeightProperty — the importer creates an
        # unweighted citation graph; asking GDS for a nonexistent 'weight'
        # property made every call fail and silently fall back.
        query = f"""
        CALL gds.pageRank.stream('{project_name}', {{
            maxIterations: 20,
            dampingFactor: 0.85
        }})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS title, score
        ORDER BY score DESC
        """
        results = session.run(query)
        return [{"title": r["title"], "score": r["score"]} for r in results]

    except Exception as e:
        logger.warning(
            "GDS PageRank unavailable (%s); falling back to in-degree scoring. "
            "Scores are NOT PageRank values.", e
        )
        query = """
        MATCH (n:Article)
        OPTIONAL MATCH (n)<-[:LINKS_TO]-(m:Article)
        WITH n, count(m) as inbound_links
        RETURN n.title AS title, toFloat(inbound_links + 1) AS score
        ORDER BY score DESC
        """
        results = session.run(query)
        return [{"title": r["title"], "score": r["score"]} for r in results]


def find_shortest_path(
    session: Any, 
    start_node_title: str, 
    end_node_title: str, 
    project_name: str = "wikipedia"
) -> List[Dict[str, Any]]:
    """
    Finds the shortest path between two nodes.

    Uses GDS Dijkstra (unweighted graph, so this is equivalent to BFS) and
    falls back to Cypher shortestPath() when GDS is not installed.
    """
    try:
        # sourceNode/targetNode take node references directly (GDS 2.x);
        # the previous version passed a property lookup via gds.util.asNode
        # and called gds.shortestPath.bfs.stream, which does not exist.
        query = f"""
        MATCH (start:Article {{title: $start_node_title}}), (end:Article {{title: $end_node_title}})
        CALL gds.shortestPath.dijkstra.stream('{project_name}', {{
            sourceNode: start,
            targetNode: end
        }})
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
        RETURN
            [nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS path,
            totalCost AS length
        """
        results = session.run(query, start_node_title=start_node_title, end_node_title=end_node_title)
        return [{"path": r["path"], "length": r["length"]} for r in results]

    except Exception as e:
        logger.warning(
            "GDS shortest path unavailable (%s); falling back to Cypher shortestPath().", e
        )
        query = """
        MATCH (start:Article {title: $start_node_title}), (end:Article {title: $end_node_title})
        MATCH p = shortestPath((start)-[:LINKS_TO*..10]->(end))
        RETURN [node IN nodes(p) | node.title] AS path, toFloat(length(p)) AS length
        """
        try:
            results = session.run(query, start_node_title=start_node_title, end_node_title=end_node_title)
            return [{"path": r["path"], "length": r["length"]} for r in results]
        except Exception as e2:
            logger.error("Cypher shortestPath fallback also failed: %s", e2)
            return []


def detect_communities(session: Any, project_name: str = "wikipedia") -> Dict[int, List[str]]:
    """
    Detects communities using the Louvain algorithm.
    Falls back to grouping by connectivity if GDS fails.
    """
    try:
        query = f"""
        CALL gds.louvain.stream('{project_name}')
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).title AS title, communityId
        ORDER BY communityId, title
        """
        results = session.run(query)

        communities: Dict[int, List[str]] = {}
        for r in results:
            community_id = r["communityId"]
            if community_id not in communities:
                communities[community_id] = []
            communities[community_id].append(r["title"])
        return communities

    except Exception as e:
        # No honest approximation of Louvain exists in plain Cypher. The old
        # fallback grouped articles by degree-mod-5, which fabricated
        # meaningless "communities" that looked like real output.
        logger.warning(
            "GDS Louvain unavailable (%s); community detection requires the "
            "GDS library — returning no communities.", e
        )
        return {}


def calculate_centrality(
    session: Any, 
    project_name: str = "wikipedia", 
    centrality_type: str = "betweenness"
) -> List[Dict[str, Any]]:
    """
    Calculates various centrality measures (betweenness, closeness).
    Raises ValueError for unsupported types.
    """
    if centrality_type not in ("betweenness", "closeness"):
        raise ValueError(f"Unsupported centrality type: {centrality_type}")

    try:
        if centrality_type == "betweenness":
            query = f"""
            CALL gds.betweenness.stream('{project_name}')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).title AS title, score
            ORDER BY score DESC
            """
        else:  # closeness
            query = f"""
            CALL gds.closeness.stream('{project_name}')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).title AS title, score
            ORDER BY score DESC
            """

        results = session.run(query)
        
        # session.run may return a list-like or an iterable result object; 
        # ensure we handle both normal iterables and mocks gracefully.
        try:
            return [{"title": r["title"], "score": r["score"]} for r in results]
        except TypeError:
            return []

    except Exception as e:
        logger.warning(
            "GDS %s centrality unavailable (%s); falling back to degree "
            "centrality. Scores are NOT %s values.",
            centrality_type, e, centrality_type,
        )
        query = """
        MATCH (n:Article)
        OPTIONAL MATCH (n)-[:LINKS_TO]-(connected)
        WITH n, count(connected) as degree
        RETURN n.title AS title, toFloat(degree) AS score
        ORDER BY score DESC
        """
        results = session.run(query)
        try:
            return [{"title": r["title"], "score": r["score"]} for r in results]
        except TypeError:
            return []


def export_results(
    data: List[Dict[str, Any]], 
    format_type: str = "json", 
    filename: str = "results"
) -> None:
    """
    Exports analysis results to a specified format (JSON or CSV).
    Ensures empty CSV files are created if data is empty.
    """
    if format_type == "json":
        with open(f"{filename}.json", "w") as f:
            json.dump(data, f, indent=4)
            
    elif format_type == "csv":
        csv_path = f"{filename}.csv"
        # Always create the CSV file. If data is empty, create an empty file.
        if not data:
            open(csv_path, "w", newline="").close()
            return
            
        with open(csv_path, "w", newline="") as f:
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            
    else:
        raise ValueError(f"Unsupported export format: {format_type}")


def measure_performance(func: Callable, *args: Any, **kwargs: Any) -> Tuple[Any, float]:
    """
    Measures the execution time of a given function.
    Returns: (function_result, duration_in_seconds)
    """
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    
    return result, (end_time - start_time)
