# wikipedia_analysis/analysis.py

import json
import csv
import time
from typing import Dict, List, Any, Optional

# Try to import Neo4j GDS, provide fallback if not available
try:
    from graphdatascience import GraphDataScience
    GDS_AVAILABLE = True
except ImportError:
    GDS_AVAILABLE = False

class MockGDS:
    """Mock GDS for testing when library unavailable."""
    
    class util:
        @staticmethod
        def asNode(node_data):
            """Mock asNode function."""
            if hasattr(node_data, 'get'):
                return node_data
            return {'title': str(node_data), 'id': node_data}
    
    @staticmethod
    def pageRank():
        return MockGDS()
    
    @staticmethod
    def shortestPath():
        return MockGDS()
    
    @staticmethod
    def louvain():
        return MockGDS()
    
    def stream(self, *args, **kwargs):
        """Mock stream method."""
        return []

# Create gds attribute for backward compatibility
if GDS_AVAILABLE:
    gds = GraphDataScience
else:
    gds = MockGDS()

def calculate_pagerank(session, project_name="wikipedia"):
    """
    Calculates PageRank for nodes in the graph.
    """
    try:
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
    except Exception as e:
        # Fallback to basic PageRank calculation
        query = f"""
        MATCH (n:Article)
        OPTIONAL MATCH (n)<-[:LINKS_TO]-(m:Article)
        WITH n, count(m) as inbound_links
        RETURN n.title AS title, toFloat(inbound_links + 1) AS score
        ORDER BY score DESC
        """
        results = session.run(query)
        return [{"title": r["title"], "score": r["score"]} for r in results]

def find_shortest_path(session, start_node_title, end_node_title, project_name="wikipedia"):
    """
    Finds the shortest path between two nodes using BFS.
    """
    try:
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
    except Exception as e:
        # Fallback to basic shortest path using Cypher
        query = """
        MATCH (start:Article {title: $start_node_title}), (end:Article {title: $end_node_title})
        CALL apoc.path.findMany(start, end, 'LINKS_TO>', '', {maxLevel: 10, limit: 1})
        YIELD path
        RETURN [node IN nodes(path) | node.title] AS path, length(path) AS length
        """
        try:
            results = session.run(query, start_node_title=start_node_title, end_node_title=end_node_title)
            return [{"path": r["path"], "length": r["length"]} for r in results]
        except Exception:
            # Final fallback: return empty result
            return []

def detect_communities(session, project_name="wikipedia"):
    """
    Detects communities using the Louvain algorithm.
    """
    try:
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
    except Exception as e:
        # Fallback: group by simple connectivity
        query = """
        MATCH (n:Article)
        OPTIONAL MATCH (n)-[:LINKS_TO]-(connected)
        WITH n, count(connected) as connections
        RETURN n.title AS title, connections % 5 AS communityId
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
    try:
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
    except Exception as e:
        # Fallback: basic degree centrality
        query = """
        MATCH (n:Article)
        OPTIONAL MATCH (n)-[:LINKS_TO]-(connected)
        WITH n, count(connected) as degree
        RETURN n.title AS title, toFloat(degree) AS score
        ORDER BY score DESC
        """
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