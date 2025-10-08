import pytest
from unittest.mock import Mock, patch
from wikipedia_analysis import analysis

# Mock the neo4j.graph.Node object for gds.util.asNode
class MockNode:
    def __init__(self, title):
        self.title = title

def mock_gds_util_asNode(node_id):
    # In a real scenario, you might map node_id to a title
    # For testing, we can just return a MockNode with a generic title or map it
    if node_id == 1:
        return MockNode("Article A")
    elif node_id == 2:
        return MockNode("Article B")
    elif node_id == 3:
        return MockNode("Article C")
    elif node_id == 4:
        return MockNode("Article D")
    return MockNode(f"Article {node_id}")

@pytest.fixture(autouse=True)
def mock_gds_util(monkeypatch):
    """
    Fixture to replace analysis.gds with a test-double that exposes util.asNode,
    avoiding issues when the real client's `util` is a property/descriptor.
    """
    class _TestGDS:
        class util:
            @staticmethod
            def asNode(node_id):
                return mock_gds_util_asNode(node_id)
    # Replace the entire gds object on the analysis module with our test double.
    monkeypatch.setattr(analysis, "gds", _TestGDS())

class MockRecord:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

class MockResult:
    def __init__(self, data):
        self._data = [MockRecord(item) for item in data]
        self._index = 0

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index < len(self._data):
            record = self._data[self._index]
            self._index += 1
            return record
        raise StopIteration

    def data(self):
        return [record._data for record in self._data]

# Test PageRank calculation functions
def test_calculate_pagerank_simple_graph(mock_neo4j_session, monkeypatch):
    """
    Test PageRank calculation with a simple graph structure.
    """
    # Mock gds.util.asNode for this specific test if needed, or rely on global mock
    # For PageRank, the query returns 'title' directly, so asNode is used within the analysis function.
    # The mock_gds_util fixture handles this.

    mock_results_data = [
        {"title": "Article A", "score": 0.85},
        {"title": "Article B", "score": 0.65},
        {"title": "Article C", "score": 0.45},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    pagerank_scores = analysis.calculate_pagerank(mock_neo4j_session)

    expected_scores = [
        {"title": "Article A", "score": 0.85},
        {"title": "Article B", "score": 0.65},
        {"title": "Article C", "score": 0.45},
    ]
    assert pagerank_scores == expected_scores
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.pageRank.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

def test_calculate_pagerank_disconnected_graph(mock_neo4j_session, monkeypatch):
    """
    Test PageRank calculation with a disconnected graph structure.
    """
    mock_results_data = [
        {"title": "Article D", "score": 0.15},
        {"title": "Article E", "score": 0.15},
        {"title": "Article F", "score": 0.15},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    pagerank_scores = analysis.calculate_pagerank(mock_neo4j_session)

    expected_scores = [
        {"title": "Article D", "score": 0.15},
        {"title": "Article E", "score": 0.15},
        {"title": "Article F", "score": 0.15},
    ]
    assert pagerank_scores == expected_scores
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.pageRank.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

def test_calculate_pagerank_empty_graph(mock_neo4j_session):
    """
    Test PageRank calculation with an empty graph (no results).
    """
    mock_neo4j_session.run.return_value = MockResult([])

    pagerank_scores = analysis.calculate_pagerank(mock_neo4j_session)

    assert pagerank_scores == []
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.pageRank.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

# Test shortest path algorithms
def test_find_shortest_path_exists(mock_neo4j_session, monkeypatch):
    """
    Test shortest path calculation when a path exists.
    """
    mock_results_data = [
        {"path": ["Article A", "Article B", "Article C"], "length": 2.0},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    path_results = analysis.find_shortest_path(mock_neo4j_session, "Article A", "Article C")

    expected_path = [{"path": ["Article A", "Article B", "Article C"], "length": 2.0}]
    assert path_results == expected_path
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.shortestPath.bfs.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]
    assert mock_neo4j_session.run.call_args[1]['start_node_title'] == "Article A"
    assert mock_neo4j_session.run.call_args[1]['end_node_title'] == "Article C"

def test_find_shortest_path_not_exists(mock_neo4j_session, monkeypatch):
    """
    Test shortest path calculation when no path exists.
    """
    mock_results_data = []
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    path_results = analysis.find_shortest_path(mock_neo4j_session, "Article X", "Article Y")

    assert path_results == []
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.shortestPath.bfs.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]
    assert mock_neo4j_session.run.call_args[1]['start_node_title'] == "Article X"
    assert mock_neo4j_session.run.call_args[1]['end_node_title'] == "Article Y"

def test_find_shortest_path_same_node(mock_neo4j_session, monkeypatch):
    """
    Test shortest path calculation when start and end nodes are the same.
    """
    mock_results_data = [
        {"path": ["Article A"], "length": 0.0},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    path_results = analysis.find_shortest_path(mock_neo4j_session, "Article A", "Article A")

    expected_path = [{"path": ["Article A"], "length": 0.0}]
    assert path_results == expected_path
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.shortestPath.bfs.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]
    assert mock_neo4j_session.run.call_args[1]['start_node_title'] == "Article A"
    assert mock_neo4j_session.run.call_args[1]['end_node_title'] == "Article A"

# Test community detection logic
def test_detect_communities_clear_structure(mock_neo4j_session, monkeypatch):
    """
    Test community detection with a clear community structure.
    """
    mock_results_data = [
        {"title": "Article A", "communityId": 1},
        {"title": "Article B", "communityId": 1},
        {"title": "Article C", "communityId": 2},
        {"title": "Article D", "communityId": 2},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    communities = analysis.detect_communities(mock_neo4j_session)

    expected_communities = {
        1: ["Article A", "Article B"],
        2: ["Article C", "Article D"],
    }
    assert communities == expected_communities
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.louvain.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

def test_detect_communities_ambiguous_structure(mock_neo4j_session, monkeypatch):
    """
    Test community detection with a more ambiguous community structure.
    """
    mock_results_data = [
        {"title": "Article A", "communityId": 1},
        {"title": "Article B", "communityId": 1},
        {"title": "Article C", "communityId": 1},
        {"title": "Article D", "communityId": 2},
        {"title": "Article E", "communityId": 2},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    communities = analysis.detect_communities(mock_neo4j_session)

    expected_communities = {
        1: ["Article A", "Article B", "Article C"],
        2: ["Article D", "Article E"],
    }
    assert communities == expected_communities
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.louvain.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

def test_detect_communities_empty_graph(mock_neo4j_session):
    """
    Test community detection with an empty graph.
    """
    mock_neo4j_session.run.return_value = MockResult([])

    communities = analysis.detect_communities(mock_neo4j_session)

    assert communities == {}
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.louvain.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

# Test centrality measure calculations
def test_calculate_centrality_betweenness(mock_neo4j_session, monkeypatch):
    """
    Test Betweenness Centrality calculation.
    """
    mock_results_data = [
        {"title": "Article A", "score": 10.5},
        {"title": "Article B", "score": 5.2},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    centrality_scores = analysis.calculate_centrality(mock_neo4j_session, centrality_type="betweenness")

    expected_scores = [
        {"title": "Article A", "score": 10.5},
        {"title": "Article B", "score": 5.2},
    ]
    assert centrality_scores == expected_scores
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.betweenness.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

def test_calculate_centrality_closeness(mock_neo4j_session, monkeypatch):
    """
    Test Closeness Centrality calculation.
    """
    mock_results_data = [
        {"title": "Article X", "score": 0.8},
        {"title": "Article Y", "score": 0.6},
    ]
    mock_neo4j_session.run.return_value = MockResult(mock_results_data)

    centrality_scores = analysis.calculate_centrality(mock_neo4j_session, centrality_type="closeness")

    expected_scores = [
        {"title": "Article X", "score": 0.8},
        {"title": "Article Y", "score": 0.6},
    ]
    assert centrality_scores == expected_scores
    mock_neo4j_session.run.assert_called_once()
    assert "CALL gds.closeness.stream('wikipedia'" in mock_neo4j_session.run.call_args[0][0]

def test_calculate_centrality_unsupported_type(mock_neo4j_session):
    """
    Test unsupported centrality type raises ValueError.
    """
    with pytest.raises(ValueError, match="Unsupported centrality type: unsupported"):
        analysis.calculate_centrality(mock_neo4j_session, centrality_type="unsupported")

# Test result formatting and export functions
def test_export_results_json(tmp_path):
    """
    Test exporting results to JSON format.
    """
    data = [{"title": "Article A", "score": 0.85}, {"title": "Article B", "score": 0.65}]
    filename = tmp_path / "test_results"
    analysis.export_results(data, format_type="json", filename=str(filename))

    with open(f"{filename}.json", "r") as f:
        exported_data = json.load(f)

    assert exported_data == data

def test_export_results_csv(tmp_path):
    """
    Test exporting results to CSV format.
    """
    data = [{"title": "Article A", "score": 0.85}, {"title": "Article B", "score": 0.65}]
    filename = tmp_path / "test_results"
    analysis.export_results(data, format_type="csv", filename=str(filename))

    with open(f"{filename}.csv", "r") as f:
        reader = csv.DictReader(f)
        exported_data = list(reader)

    expected_data = [
        {"title": "Article A", "score": "0.85"},
        {"title": "Article B", "score": "0.65"},
    ]
    assert exported_data == expected_data

def test_export_results_unsupported_format():
    """
    Test unsupported export format raises ValueError.
    """
    data = [{"title": "Article A", "score": 0.85}]
    with pytest.raises(ValueError, match="Unsupported export format: txt"):
        analysis.export_results(data, format_type="txt", filename="test_results")

def test_export_results_csv_empty_data(tmp_path):
    """
    Test exporting empty data to CSV format.
    """
    data = []
    filename = tmp_path / "empty_results"
    analysis.export_results(data, format_type="csv", filename=str(filename))

    # Check that the file is created but might be empty or only have headers
    with open(f"{filename}.csv", "r") as f:
        content = f.read()
    assert content == "" # Or just headers, depending on csv.DictWriter behavior with empty rows

# Test performance measurement
def test_measure_performance():
    """
    Test the performance measurement utility function.
    """
    def dummy_function(delay):
        time.sleep(delay)
        return "done"

    result, duration = analysis.measure_performance(dummy_function, 0.1)

    assert result == "done"
    assert duration >= 0.1
    assert duration < 0.2 # Should be close to 0.1, but allow for some overhead