"""
Benchmark tests for Wikipedia analysis operations.

Run with:  pytest tests/test_benchmarks.py -m benchmark
Neo4j-dependent benchmarks also require: -m "benchmark and integration"

Results are written to benchmarks/results/<timestamp>.json at session end.
"""
import pytest
from pathlib import Path

from benchmarks.runner import BenchmarkRunner, BenchmarkResult
from benchmarks.reporter import write_report
from wikipedia_analysis.analysis import (
    calculate_pagerank,
    find_shortest_path,
    detect_communities,
    calculate_centrality,
)
from wikipedia_analysis.data_processing import (
    parse_dump_file,
    batch_data,
    transform_to_article_node,
)
from wikipedia_analysis.database import batch_import_nodes, batch_import_relationships
from wikipedia_analysis import queries

SAMPLE_DATA_XML = Path(__file__).parent / "fixtures" / "sample_data.xml"
BENCH_GRAPH = "bench_graph"
_results: list[BenchmarkResult] = []


# ---------------------------------------------------------------------------
# Session fixture: project GDS graph once; collect & write report on teardown
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def gds_graph(populated_neo4j_db):
    """Projects a single GDS in-memory graph shared across all algorithm benchmarks."""
    driver = populated_neo4j_db
    with driver.session() as session:
        session.run(f"""
            CALL gds.graph.project(
                '{BENCH_GRAPH}',
                ['Article', 'Category'],
                {{
                    LINKS_TO: {{orientation: 'UNDIRECTED'}},
                    BELONGS_TO: {{orientation: 'UNDIRECTED'}},
                    REDIRECTS_TO: {{orientation: 'UNDIRECTED'}}
                }}
            )
        """)
    yield driver
    with driver.session() as session:
        session.run(f"CALL gds.graph.drop('{BENCH_GRAPH}')")


@pytest.fixture(scope="session", autouse=True)
def _write_report(request):
    yield
    if _results:
        write_report(
            _results,
            dataset_info={"source": str(SAMPLE_DATA_XML), "articles": 4, "categories": 4},
        )


# ---------------------------------------------------------------------------
# GDS / Cypher algorithm benchmarks (require Neo4j + GDS)
# ---------------------------------------------------------------------------

@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_calculate_pagerank(gds_graph):
    driver = gds_graph
    with driver.session() as session:
        runner = BenchmarkRunner("calculate_pagerank", category="graph_algorithm", repeats=5)
        result = runner.run(calculate_pagerank, session, project_name=BENCH_GRAPH)
    _results.append(result)
    assert result.errors < result.repeats


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_find_shortest_path(gds_graph):
    driver = gds_graph
    with driver.session() as session:
        runner = BenchmarkRunner("find_shortest_path", category="graph_algorithm", repeats=5)
        result = runner.run(find_shortest_path, session, "Article A", "Article B", project_name=BENCH_GRAPH)
    _results.append(result)
    assert result.errors < result.repeats


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_detect_communities(gds_graph):
    driver = gds_graph
    with driver.session() as session:
        runner = BenchmarkRunner("detect_communities", category="graph_algorithm", repeats=5)
        result = runner.run(detect_communities, session, project_name=BENCH_GRAPH)
    _results.append(result)
    assert result.errors < result.repeats


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_calculate_centrality_betweenness(gds_graph):
    driver = gds_graph
    with driver.session() as session:
        runner = BenchmarkRunner("calculate_centrality_betweenness", category="graph_algorithm", repeats=5)
        result = runner.run(calculate_centrality, session, project_name=BENCH_GRAPH, centrality_type="betweenness")
    _results.append(result)
    assert result.errors < result.repeats


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_calculate_centrality_closeness(gds_graph):
    driver = gds_graph
    with driver.session() as session:
        runner = BenchmarkRunner("calculate_centrality_closeness", category="graph_algorithm", repeats=5)
        result = runner.run(calculate_centrality, session, project_name=BENCH_GRAPH, centrality_type="closeness")
    _results.append(result)
    assert result.errors < result.repeats


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_gds_graph_projection(populated_neo4j_db):
    """Benchmarks full project+drop cycle for GDS graph projection."""
    driver = populated_neo4j_db
    PROJ = "bench_proj_cycle"

    def project_and_drop():
        with driver.session() as session:
            session.run(f"""
                CALL gds.graph.project(
                    '{PROJ}',
                    ['Article', 'Category'],
                    {{LINKS_TO: {{orientation: 'UNDIRECTED'}}}}
                )
            """)
            session.run(f"CALL gds.graph.drop('{PROJ}')")

    runner = BenchmarkRunner("gds_graph_projection", category="graph_algorithm", repeats=3)
    result = runner.run(project_and_drop)
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_find_most_authoritative_articles(populated_neo4j_db):
    driver = populated_neo4j_db
    QUERY = """
    MATCH (a:Article)
    WITH a, size((a)<-[:LINKS_TO]-()) AS in_degree
    RETURN a.title AS article, in_degree
    ORDER BY in_degree DESC LIMIT 20
    """

    def run_query():
        with driver.session() as session:
            return list(session.run(QUERY))

    runner = BenchmarkRunner("find_most_authoritative_articles", category="cypher", repeats=10)
    result = runner.run(run_query)
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_batch_import_nodes(populated_neo4j_db):
    driver = populated_neo4j_db

    for size in [10, 100, 1000]:
        nodes = [{"id": f"bench_{i}", "title": f"Bench Article {i}"} for i in range(size)]

        def do_import():
            with driver.session() as session:
                batch_import_nodes(session, "Article", nodes)

        runner = BenchmarkRunner(f"batch_import_nodes_{size}", category="import", repeats=3)
        result = runner.run_with_throughput(size, do_import)
        _results.append(result)

        # cleanup
        with driver.session() as session:
            session.run("MATCH (a:Article) WHERE a.id STARTS WITH 'bench_' DETACH DELETE a")

    assert True


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_batch_import_relationships(populated_neo4j_db):
    driver = populated_neo4j_db

    # Pre-seed source and target nodes
    with driver.session() as session:
        src_nodes = [{"id": f"rel_src_{i}", "title": f"Src {i}"} for i in range(10)]
        tgt_nodes = [{"id": f"rel_tgt_{i}", "title": f"Tgt {i}"} for i in range(10)]
        batch_import_nodes(session, "Article", src_nodes)
        batch_import_nodes(session, "Article", tgt_nodes)

    rels = [{"src_id": f"rel_src_{i}", "tgt_id": f"rel_tgt_{i}"} for i in range(10)]

    def do_import():
        with driver.session() as session:
            batch_import_relationships(session, "LINKS_TO", "Article", "Article", "src_id", "tgt_id", rels)

    runner = BenchmarkRunner("batch_import_relationships_10", category="import", repeats=5)
    result = runner.run_with_throughput(len(rels), do_import)
    _results.append(result)

    with driver.session() as session:
        session.run("MATCH (a:Article) WHERE a.id STARTS WITH 'rel_' DETACH DELETE a")

    assert result.errors == 0


# ---------------------------------------------------------------------------
# Data processing benchmarks (no Neo4j)
# ---------------------------------------------------------------------------

@pytest.mark.benchmark
def test_bench_parse_dump_file():
    if not SAMPLE_DATA_XML.exists():
        pytest.skip("sample_data.xml not found")

    runner = BenchmarkRunner("parse_dump_file", category="data_processing", repeats=10)
    result = runner.run(lambda: list(parse_dump_file(str(SAMPLE_DATA_XML))))
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
def test_bench_batch_data_varying_sizes():
    items = list(range(500))

    for size in [10, 50, 100, 500]:
        runner = BenchmarkRunner(f"batch_data_size_{size}", category="data_processing", repeats=10)
        result = runner.run(lambda s=size: list(batch_data(iter(items), s)))
        _results.append(result)

    assert True


@pytest.mark.benchmark
def test_bench_transform_to_article_node():
    bulk = [
        {"id": str(i), "title": f"Article {i}", "url": f"https://en.wikipedia.org/wiki/Article_{i}", "links": []}
        for i in range(1000)
    ]
    runner = BenchmarkRunner("transform_to_article_node_1000", category="data_processing", repeats=10)
    result = runner.run_with_throughput(1000, lambda: [transform_to_article_node(a) for a in bulk])
    _results.append(result)
    assert result.errors == 0


# ---------------------------------------------------------------------------
# API endpoint benchmarks (Flask test client + populated_neo4j_db)
# ---------------------------------------------------------------------------

@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_api_get_categories(populated_neo4j_db):
    import os
    os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
    os.environ.setdefault("NEO4J_USER", "neo4j")
    os.environ.setdefault("NEO4J_PASSWORD", "testpassword")

    from wikipedia_analysis.api import app
    app.config["TESTING"] = True

    driver = populated_neo4j_db

    def call_endpoint():
        with app.test_client() as c:
            from unittest.mock import MagicMock, patch
            mock_sess = MagicMock()
            mock_sess.run.return_value = iter([{"categoryName": "Test Category"}])
            mock_sess.__enter__ = lambda s: s
            mock_sess.__exit__ = MagicMock(return_value=False)
            with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
                return c.get("/categories")

    runner = BenchmarkRunner("api_get_categories", category="api", repeats=10)
    result = runner.run(call_endpoint)
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
@pytest.mark.integration
def test_bench_api_get_category_articles(populated_neo4j_db):
    from wikipedia_analysis.api import app
    app.config["TESTING"] = True

    def call_endpoint():
        with app.test_client() as c:
            from unittest.mock import MagicMock, patch
            mock_sess = MagicMock()
            mock_sess.run.return_value = iter([{"articleTitle": "Article A"}])
            mock_sess.__enter__ = lambda s: s
            mock_sess.__exit__ = MagicMock(return_value=False)
            with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
                return c.get("/category/Test%20Category")

    runner = BenchmarkRunner("api_get_category_articles", category="api", repeats=10)
    result = runner.run(call_endpoint)
    _results.append(result)
    assert result.errors == 0


# ---------------------------------------------------------------------------
# Query builder benchmarks (pure Python)
# ---------------------------------------------------------------------------

@pytest.mark.benchmark
def test_bench_build_pagerank_query():
    runner = BenchmarkRunner("build_pagerank_query", category="query_builder", repeats=10)
    result = runner.run(queries.build_pagerank_query)
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
def test_bench_build_shortest_path_query():
    runner = BenchmarkRunner("build_shortest_path_query", category="query_builder", repeats=10)
    result = runner.run(queries.build_shortest_path_query, "Article A", "Article B")
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
def test_bench_build_community_detection_query():
    runner = BenchmarkRunner("build_community_detection_query", category="query_builder", repeats=10)
    result = runner.run(queries.build_community_detection_query)
    _results.append(result)
    assert result.errors == 0


@pytest.mark.benchmark
def test_bench_build_batch_create_articles_query():
    for size in [10, 100, 1000]:
        article_data = [{"title": f"Article {i}", "namespace": 0, "length": i * 10} for i in range(size)]
        runner = BenchmarkRunner(f"build_batch_create_articles_query_{size}", category="query_builder", repeats=10)
        result = runner.run(queries.build_batch_create_articles_query, article_data)
        _results.append(result)
    assert True
