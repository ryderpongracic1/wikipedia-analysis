import pytest
from neo4j import GraphDatabase
import time
from wikipedia_analysis.database import batch_import_nodes, batch_import_relationships
from wikipedia_analysis.analysis import calculate_pagerank, find_shortest_path, detect_communities, export_results, measure_performance
from pathlib import Path
import os

# Define the path to the sample data XML file
SAMPLE_DATA_XML = Path(__file__).parent / "fixtures" / "sample_data.xml"

@pytest.mark.integration
def test_end_to_end_data_import_workflow(populated_neo4j_db):
    """
    Tests the end-to-end data import workflow, verifying node and relationship counts
    and data integrity.
    """
    driver = populated_neo4j_db
    with driver.session() as session:
        # Verify node counts
        article_count = session.run("MATCH (a:Article) RETURN count(a) AS count").single()["count"]
        category_count = session.run("MATCH (c:Category) RETURN count(c) AS count").single()["count"]
        
        # Based on sample_data.xml:
        # Articles: "Article A", "Article B", "Article C", "Redirect Article" (4)
        # Categories: "Test Category", "Empty Category" (2)
        assert article_count == 4
        assert category_count == 2

        # Verify relationship counts
        links_to_count = session.run("MATCH ()-[:LINKS_TO]->() RETURN count(*) AS count").single()["count"]
        belongs_to_count = session.run("MATCH ()-[:BELONGS_TO]->() RETURN count(*) AS count").single()["count"]
        redirects_to_count = session.run("MATCH ()-[:REDIRECTS_TO]->() RETURN count(*) AS count").single()["count"]

        # Based on sample_data.xml:
        # Article A -> Article B (1)
        # Article B -> Article A (1)
        # Article A -> Category:Test Category (1)
        # Redirect Article -> Article A (1)
        assert links_to_count == 2
        assert belongs_to_count == 1
        assert redirects_to_count == 1

        # Query for specific articles and relationships
        result_article_a = session.run("MATCH (a:Article {title: 'Article A'}) RETURN a.id AS id").single()
        assert result_article_a is not None

        result_category_test = session.run("MATCH (c:Category {name: 'Test Category'}) RETURN c.id AS id").single()
        assert result_category_test is not None

        # Verify link from Article A to Article B
        link_ab = session.run("""
            MATCH (a:Article {title: 'Article A'})-[:LINKS_TO]->(b:Article {title: 'Article B'})
            RETURN count(*) AS count
        """).single()["count"]
        assert link_ab == 1

        # Verify Article A belongs to Test Category
        belongs_ac = session.run("""
            MATCH (a:Article {title: 'Article A'})-[:BELONGS_TO]->(c:Category {name: 'Test Category'})
            RETURN count(*) AS count
        """).single()["count"]
        assert belongs_ac == 1

        # Verify Redirect Article redirects to Article A
        redirect_ra = session.run("""
            MATCH (r:Article {title: 'Redirect Article'})-[:REDIRECTS_TO]->(a:Article {title: 'Article A'})
            RETURN count(*) AS count
        """).single()["count"]
        assert redirect_ra == 1

        # Test data integrity: Attempt to import duplicate data
        # The constraints should prevent duplicate nodes based on 'id'
        duplicate_articles = [{'id': '1', 'title': 'Article A Duplicate', 'is_redirect': False, 'redirect_title': None}]
        batch_import_nodes(session, "Article", duplicate_articles)
        
        # Verify article count remains the same (due to unique constraint on id)
        new_article_count = session.run("MATCH (a:Article) RETURN count(a) AS count").single()["count"]
        assert new_article_count == article_count

        # Attempt to create a duplicate relationship (MERGE handles this gracefully)
        duplicate_links_to = [{'source_article_id': '1', 'target_article_id': '2'}]
        batch_import_relationships(session, "LINKS_TO", "Article", "Article", "source_article_id", "target_article_id", duplicate_links_to)
        
        new_links_to_count = session.run("MATCH ()-[:LINKS_TO]->() RETURN count(*) AS count").single()["count"]
        assert new_links_to_count == links_to_count

@pytest.mark.integration
def test_complete_analysis_pipeline(populated_neo4j_db):
    """
    Tests the complete analysis pipeline including PageRank, Shortest Path, and Community Detection.
    """
    driver = populated_neo4j_db
    with driver.session() as session:
        # Ensure the graph is projected for GDS algorithms
        # This is a simplified projection for testing; in a real scenario,
        # you might project once for all tests or within a dedicated fixture.
        session.run("""
            CALL gds.graph.project(
                'test_graph',
                ['Article', 'Category'],
                {
                    LINKS_TO: {orientation: 'UNDIRECTED'},
                    BELONGS_TO: {orientation: 'UNDIRECTED'},
                    REDIRECTS_TO: {orientation: 'UNDIRECTED'}
                }
            )
        """)

        # Test PageRank calculation
        pagerank_results = calculate_pagerank(session, project_name='test_graph')
        assert len(pagerank_results) > 0
        # Basic check: Article A and B should have higher PageRank due to mutual links
        article_a_pr = next((item['score'] for item in pagerank_results if item['title'] == 'Article A'), 0)
        article_b_pr = next((item['score'] for item in pagerank_results if item['title'] == 'Article B'), 0)
        article_c_pr = next((item['score'] for item in pagerank_results if item['title'] == 'Article C'), 0)
        assert article_a_pr > article_c_pr
        assert article_b_pr > article_c_pr

        # Test Shortest Path
        shortest_path_results = find_shortest_path(session, 'Article A', 'Article B', project_name='test_graph')
        assert len(shortest_path_results) > 0
        assert shortest_path_results[0]['path'] == ['Article A', 'Article B']
        assert shortest_path_results[0]['length'] == 1.0 # Assuming default weight of 1

        # Test Community Detection
        community_results = detect_communities(session, project_name='test_graph')
        assert len(community_results) > 0
        # Verify that Article A and Article B are likely in the same community
        # This is a heuristic check, as community detection can be non-deterministic
        found_a_community = None
        found_b_community = None
        for community_id, members in community_results.items():
            if 'Article A' in members:
                found_a_community = community_id
            if 'Article B' in members:
                found_b_community = community_id
        assert found_a_community is not None
        assert found_b_community is not None
        assert found_a_community == found_b_community # A and B should be in the same community

        # Clean up the projected graph
        session.run("CALL gds.graph.drop('test_graph')")

@pytest.mark.integration
def test_data_export_functionality(populated_neo4j_db, tmp_path):
    """
    Tests the data export functionality by exporting PageRank results to a temporary file.
    """
    driver = populated_neo4j_db
    with driver.session() as session:
        session.run("""
            CALL gds.graph.project(
                'export_graph',
                ['Article', 'Category'],
                {
                    LINKS_TO: {orientation: 'UNDIRECTED'},
                    BELONGS_TO: {orientation: 'UNDIRECTED'},
                    REDIRECTS_TO: {orientation: 'UNDIRECTED'}
                }
            )
        """)
        
        pagerank_results = calculate_pagerank(session, project_name='export_graph')
        
        # Export to JSON
        json_file = tmp_path / "pagerank_results.json"
        export_results(pagerank_results, format_type="json", filename=str(json_file.with_suffix('')))
        
        assert json_file.exists()
        with open(json_file, 'r') as f:
            exported_data = json.load(f)
            assert len(exported_data) == len(pagerank_results)
            assert exported_data[0]['title'] == pagerank_results[0]['title'] # Check first item

        # Export to CSV
        csv_file = tmp_path / "pagerank_results.csv"
        export_results(pagerank_results, format_type="csv", filename=str(csv_file.with_suffix('')))

        assert csv_file.exists()
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            csv_data = list(reader)
            assert len(csv_data) == len(pagerank_results)
            assert csv_data[0]['title'] == pagerank_results[0]['title']

        session.run("CALL gds.graph.drop('export_graph')")

@pytest.mark.integration
def test_performance_benchmarking(populated_neo4j_db):
    """
    Measures the time taken for the end-to-end data import and analysis pipeline.
    This is a basic benchmark and can be expanded.
    """
    driver = populated_neo4j_db
    with driver.session() as session:
        # Measure graph projection time
        _, projection_time = measure_performance(
            session.run,
            """
            CALL gds.graph.project(
                'benchmark_graph',
                ['Article', 'Category'],
                {
                    LINKS_TO: {orientation: 'UNDIRECTED'},
                    BELONGS_TO: {orientation: 'UNDIRECTED'},
                    REDIRECTS_TO: {orientation: 'UNDIRECTED'}
                }
            )
            """
        )
        print(f"\nGraph Projection Time: {projection_time:.4f} seconds")

        # Measure PageRank calculation time
        _, pagerank_time = measure_performance(calculate_pagerank, session, project_name='benchmark_graph')
        print(f"PageRank Calculation Time: {pagerank_time:.4f} seconds")

        # Measure Shortest Path calculation time (example between two articles)
        _, shortest_path_time = measure_performance(find_shortest_path, session, 'Article A', 'Article B', project_name='benchmark_graph')
        print(f"Shortest Path Calculation Time: {shortest_path_time:.4f} seconds")

        # Measure Community Detection time
        _, community_detection_time = measure_performance(detect_communities, session, project_name='benchmark_graph')
        print(f"Community Detection Time: {community_detection_time:.4f} seconds")

        total_analysis_time = projection_time + pagerank_time + shortest_path_time + community_detection_time
        print(f"Total Analysis Pipeline Time: {total_analysis_time:.4f} seconds")

        assert projection_time > 0
        assert pagerank_time > 0
        assert shortest_path_time > 0
        assert community_detection_time > 0

        session.run("CALL gds.graph.drop('benchmark_graph')")

# Error Handling Tests (Conceptual - direct container manipulation is complex)
# For robust error handling tests, one would typically mock the driver/session
# to simulate connection errors or database unavailability.
# Example of a conceptual test:
@pytest.mark.integration
def test_error_handling_database_connection_failure_mocked(populated_neo4j_db, monkeypatch):
    """
    Conceptual test for database connection failure using mocking.
    In a real scenario, you'd mock the GraphDatabase.driver or session.run to raise exceptions.
    """
    driver = populated_neo4j_db
    
    # Mock the session.run method to raise an exception
    class MockSession:
        def run(self, *args, **kwargs):
            if "CREATE CONSTRAINT" in args[0] or "MATCH (n) DETACH DELETE n" in args[0]:
                # Allow setup/teardown queries to pass
                return
            raise Exception("Simulated database connection error")
        
        def close(self):
            pass

    class MockDriver:
        def session(self):
            return MockSession()
        def verify_connectivity(self):
            pass
        def close(self):
            pass

    monkeypatch.setattr(GraphDatabase, 'driver', lambda *args, **kwargs: MockDriver())

    # Now, attempting to use the driver should raise the mocked error
    with pytest.raises(Exception, match="Simulated database connection error"):
        with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j")).session() as session:
            session.run("MATCH (n) RETURN n")