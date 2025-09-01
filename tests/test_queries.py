import pytest
from wikipedia_analysis import queries

def test_build_article_query_no_filters():
    """
    Test building an Article query without any filters.
    """
    expected_query = "MATCH (a:Article) RETURN a"
    assert queries.build_article_query() == expected_query

def test_build_article_query_with_title():
    """
    Test building an Article query with a title filter.
    """
    expected_query = "MATCH (a:Article {title: 'Test Article'}) RETURN a"
    assert queries.build_article_query(title='Test Article') == expected_query

def test_build_article_query_with_namespace():
    """
    Test building an Article query with a namespace filter.
    """
    expected_query = "MATCH (a:Article {namespace: 0}) RETURN a"
    assert queries.build_article_query(namespace=0) == expected_query

def test_build_article_query_with_length():
    """
    Test building an Article query with a length filter.
    """
    expected_query = "MATCH (a:Article {length: 1000}) RETURN a"
    assert queries.build_article_query(length=1000) == expected_query

def test_build_article_query_with_multiple_filters():
    """
    Test building an Article query with multiple filters.
    """
    expected_query = "MATCH (a:Article {title: 'Another Article', namespace: 1, length: 500}) RETURN a"
    assert queries.build_article_query(title='Another Article', namespace=1, length=500) == expected_query

def test_build_category_query_no_filters():
    """
    Test building a Category query without any filters.
    """
    expected_query = "MATCH (c:Category) RETURN c"
    assert queries.build_category_query() == expected_query

def test_build_category_query_with_name():
    """
    Test building a Category query with a name filter.
    """
    expected_query = "MATCH (c:Category {name: 'Test Category'}) RETURN c"
    assert queries.build_category_query(name='Test Category') == expected_query

def test_build_links_to_query():
    """
    Test building a LINKS_TO relationship query.
    """
    expected_query = (
        "MATCH (from:Article {title: 'Article A'}), "
        "(to:Article {title: 'Article B'}) "
        "MERGE (from)-[:LINKS_TO]->(to)"
    )
    assert queries.build_links_to_query('Article A', 'Article B') == expected_query

def test_build_belongs_to_query():
    """
    Test building a BELONGS_TO relationship query.
    """
    expected_query = (
        "MATCH (a:Article {title: 'Article X'}), "
        "(c:Category {name: 'Category Y'}) "
        "MERGE (a)-[:BELONGS_TO]->(c)"
    )
    assert queries.build_belongs_to_query('Article X', 'Category Y') == expected_query

def test_build_redirects_to_query():
    """
    Test building a REDIRECTS_TO relationship query.
    """
    expected_query = (
        "MATCH (from:Article {title: 'Old Article'}), "
        "(to:Article {title: 'New Article'}) "
        "MERGE (from)-[:REDIRECTS_TO]->(to)"
    )
    assert queries.build_redirects_to_query('Old Article', 'New Article') == expected_query

def test_build_pagerank_query_default_params():
    """
    Test building a PageRank query with default parameters.
    """
    expected_query = (
        "CALL gds.pageRank.stream('wikiGraph', { "
        "maxIterations: 20, dampingFactor: 0.85 "
        "}) YIELD nodeId, score "
        "RETURN gds.util.asNode(nodeId).title AS article, score "
        "ORDER BY score DESC"
    )
    assert queries.build_pagerank_query() == expected_query

def test_build_pagerank_query_custom_params():
    """
    Test building a PageRank query with custom parameters.
    """
    expected_query = (
        "CALL gds.pageRank.stream('wikiGraph', { "
        "maxIterations: 10, dampingFactor: 0.7 "
        "}) YIELD nodeId, score "
        "RETURN gds.util.asNode(nodeId).title AS article, score "
        "ORDER BY score DESC"
    )
    assert queries.build_pagerank_query(max_iterations=10, damping_factor=0.7) == expected_query

def test_build_shortest_path_query_default_relationship():
    """
    Test building a shortest path query with default relationship type.
    """
    expected_query = (
        "MATCH (start:Article {title: 'Start Article'}), "
        "(end:Article {title: 'End Article'}) "
        "CALL gds.shortestPath.dijkstra.stream('wikiGraph', { "
        "sourceNode: gds.util.asNode(start).id, "
        "targetNode: gds.util.asNode(end).id, "
        "relationshipWeightProperty: 'weight', "
        "relationshipType: 'LINKS_TO' "
        "}) YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path "
        "RETURN "
        "gds.util.asNode(sourceNode).title AS source, "
        "gds.util.asNode(targetNode).title AS target, "
        "totalCost, "
        "[nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS nodesInPath, "
        "costs "
        "ORDER BY index"
    )
    assert queries.build_shortest_path_query('Start Article', 'End Article') == expected_query

def test_build_shortest_path_query_custom_relationship():
    """
    Test building a shortest path query with a custom relationship type.
    """
    expected_query = (
        "MATCH (start:Article {title: 'Start Article'}), "
        "(end:Article {title: 'End Article'}) "
        "CALL gds.shortestPath.dijkstra.stream('wikiGraph', { "
        "sourceNode: gds.util.asNode(start).id, "
        "targetNode: gds.util.asNode(end).id, "
        "relationshipWeightProperty: 'weight', "
        "relationshipType: 'CUSTOM_REL' "
        "}) YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path "
        "RETURN "
        "gds.util.asNode(sourceNode).title AS source, "
        "gds.util.asNode(targetNode).title AS target, "
        "totalCost, "
        "[nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS nodesInPath, "
        "costs "
        "ORDER BY index"
    )
    assert queries.build_shortest_path_query('Start Article', 'End Article', relationship_type='CUSTOM_REL') == expected_query

def test_build_community_detection_query_louvain():
    """
    Test building a Louvain community detection query.
    """
    expected_query = (
        "CALL gds.louvain.stream('wikiGraph') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).title AS article, communityId "
        "ORDER BY communityId, article"
    )
    assert queries.build_community_detection_query(algorithm='louvain') == expected_query

def test_build_community_detection_query_label_propagation():
    """
    Test building a Label Propagation community detection query.
    """
    expected_query = (
        "CALL gds.labelPropagation.stream('wikiGraph') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).title AS article, communityId "
        "ORDER BY communityId, article"
    )
    assert queries.build_community_detection_query(algorithm='label_propagation') == expected_query

def test_build_community_detection_query_unsupported_algorithm():
    """
    Test building a community detection query with an unsupported algorithm.
    """
    with pytest.raises(ValueError, match="Unsupported community detection algorithm: unknown"):
        queries.build_community_detection_query(algorithm='unknown')

def test_build_batch_create_articles_query_empty_list():
    """
    Test building a batch create articles query with an empty list.
    """
    assert queries.build_batch_create_articles_query([]) == ""

def test_build_batch_create_articles_query_single_article():
    """
    Test building a batch create articles query with a single article.
    """
    article_data = [{'title': 'Article 1', 'namespace': 0, 'length': 100}]
    expected_query = (
        "UNWIND $props AS article_props\n"
        "CREATE (a:Article)\n"
        "SET a = article_props\n"
        "RETURN a"
    )
    assert queries.build_batch_create_articles_query(article_data) == expected_query

def test_build_batch_create_articles_query_multiple_articles():
    """
    Test building a batch create articles query with multiple articles.
    """
    article_data = [
        {'title': 'Article 1', 'namespace': 0, 'length': 100},
        {'title': 'Article 2', 'namespace': 1, 'length': 200}
    ]
    expected_query = (
        "UNWIND $props AS article_props\n"
        "CREATE (a:Article)\n"
        "SET a = article_props\n"
        "RETURN a"
    )
    assert queries.build_batch_create_articles_query(article_data) == expected_query

def test_build_batch_create_links_query_empty_list():
    """
    Test building a batch create links query with an empty list.
    """
    assert queries.build_batch_create_links_query([]) == ""

def test_build_batch_create_links_query_single_link():
    """
    Test building a batch create links query with a single link.
    """
    link_data = [{'from_title': 'Article A', 'to_title': 'Article B'}]
    expected_query = (
        "UNWIND $props AS link_props\n"
        "MATCH (from:Article {title: link_props.from_title})\n"
        "MATCH (to:Article {title: link_props.to_title})\n"
        "MERGE (from)-[:LINKS_TO]->(to)"
    )
    assert queries.build_batch_create_links_query(link_data) == expected_query

def test_build_batch_create_links_query_multiple_links():
    """
    Test building a batch create links query with multiple links.
    """
    link_data = [
        {'from_title': 'Article A', 'to_title': 'Article B'},
        {'from_title': 'Article C', 'to_title': 'Article D'}
    ]
    expected_query = (
        "UNWIND $props AS link_props\n"
        "MATCH (from:Article {title: link_props.from_title})\n"
        "MATCH (to:Article {title: link_props.to_title})\n"
        "MERGE (from)-[:LINKS_TO]->(to)"
    )
    assert queries.build_batch_create_links_query(link_data) == expected_query
