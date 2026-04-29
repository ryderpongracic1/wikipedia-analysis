import pytest
from wikipedia_analysis import queries


# ---------------------------------------------------------------------------
# build_article_query — returns (query, params)
# ---------------------------------------------------------------------------

def test_build_article_query_no_filters():
    assert queries.build_article_query() == ("MATCH (a:Article) RETURN a", {})


def test_build_article_query_with_title():
    q, p = queries.build_article_query(title='Test Article')
    assert q == "MATCH (a:Article {title: $title}) RETURN a"
    assert p == {"title": "Test Article"}


def test_build_article_query_with_namespace():
    q, p = queries.build_article_query(namespace=0)
    assert q == "MATCH (a:Article {namespace: $namespace}) RETURN a"
    assert p == {"namespace": 0}


def test_build_article_query_with_length():
    q, p = queries.build_article_query(length=1000)
    assert q == "MATCH (a:Article {length: $length}) RETURN a"
    assert p == {"length": 1000}


def test_build_article_query_with_multiple_filters():
    q, p = queries.build_article_query(title='Another Article', namespace=1, length=500)
    assert q == "MATCH (a:Article {title: $title, namespace: $namespace, length: $length}) RETURN a"
    assert p == {"title": "Another Article", "namespace": 1, "length": 500}


# ---------------------------------------------------------------------------
# build_category_query — returns (query, params)
# ---------------------------------------------------------------------------

def test_build_category_query_no_filters():
    assert queries.build_category_query() == ("MATCH (c:Category) RETURN c", {})


def test_build_category_query_with_name():
    q, p = queries.build_category_query(name='Test Category')
    assert q == "MATCH (c:Category {name: $name}) RETURN c"
    assert p == {"name": "Test Category"}


# ---------------------------------------------------------------------------
# build_links_to_query — returns (query, params)
# ---------------------------------------------------------------------------

def test_build_links_to_query():
    q, p = queries.build_links_to_query('Article A', 'Article B')
    assert q == (
        "MATCH (from:Article {title: $from_title}), "
        "(to:Article {title: $to_title}) "
        "MERGE (from)-[:LINKS_TO]->(to)"
    )
    assert p == {"from_title": "Article A", "to_title": "Article B"}


# ---------------------------------------------------------------------------
# build_belongs_to_query — returns (query, params)
# ---------------------------------------------------------------------------

def test_build_belongs_to_query():
    q, p = queries.build_belongs_to_query('Article X', 'Category Y')
    assert q == (
        "MATCH (a:Article {title: $article_title}), "
        "(c:Category {name: $category_name}) "
        "MERGE (a)-[:BELONGS_TO]->(c)"
    )
    assert p == {"article_title": "Article X", "category_name": "Category Y"}


# ---------------------------------------------------------------------------
# build_redirects_to_query — returns (query, params)
# ---------------------------------------------------------------------------

def test_build_redirects_to_query():
    q, p = queries.build_redirects_to_query('Old Article', 'New Article')
    assert q == (
        "MATCH (from:Article {title: $from_title}), "
        "(to:Article {title: $to_title}) "
        "MERGE (from)-[:REDIRECTS_TO]->(to)"
    )
    assert p == {"from_title": "Old Article", "to_title": "New Article"}


# ---------------------------------------------------------------------------
# build_pagerank_query — returns plain string (no user input)
# ---------------------------------------------------------------------------

def test_build_pagerank_query_default_params():
    expected_query = (
        "CALL gds.pageRank.stream('wikiGraph', { "
        "maxIterations: 20, dampingFactor: 0.85 "
        "}) YIELD nodeId, score "
        "RETURN gds.util.asNode(nodeId).title AS article, score "
        "ORDER BY score DESC"
    )
    assert queries.build_pagerank_query() == expected_query


def test_build_pagerank_query_custom_params():
    expected_query = (
        "CALL gds.pageRank.stream('wikiGraph', { "
        "maxIterations: 10, dampingFactor: 0.7 "
        "}) YIELD nodeId, score "
        "RETURN gds.util.asNode(nodeId).title AS article, score "
        "ORDER BY score DESC"
    )
    assert queries.build_pagerank_query(max_iterations=10, damping_factor=0.7) == expected_query


# ---------------------------------------------------------------------------
# build_shortest_path_query — returns (query, params)
# ---------------------------------------------------------------------------

def test_build_shortest_path_query_default_relationship():
    q, p = queries.build_shortest_path_query('Start Article', 'End Article')
    assert "title: $start_title" in q
    assert "title: $end_title" in q
    assert "relationshipType: 'LINKS_TO'" in q
    assert p == {"start_title": "Start Article", "end_title": "End Article"}


def test_build_shortest_path_query_custom_relationship():
    q, p = queries.build_shortest_path_query('Start Article', 'End Article', relationship_type='CUSTOM_REL')
    assert "relationshipType: 'CUSTOM_REL'" in q
    assert p == {"start_title": "Start Article", "end_title": "End Article"}


# ---------------------------------------------------------------------------
# build_community_detection_query — returns plain string
# ---------------------------------------------------------------------------

def test_build_community_detection_query_louvain():
    expected_query = (
        "CALL gds.louvain.stream('wikiGraph') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).title AS article, communityId "
        "ORDER BY communityId, article"
    )
    assert queries.build_community_detection_query(algorithm='louvain') == expected_query


def test_build_community_detection_query_label_propagation():
    expected_query = (
        "CALL gds.labelPropagation.stream('wikiGraph') "
        "YIELD nodeId, communityId "
        "RETURN gds.util.asNode(nodeId).title AS article, communityId "
        "ORDER BY communityId, article"
    )
    assert queries.build_community_detection_query(algorithm='label_propagation') == expected_query


def test_build_community_detection_query_unsupported_algorithm():
    with pytest.raises(ValueError, match="Unsupported community detection algorithm: unknown"):
        queries.build_community_detection_query(algorithm='unknown')


# ---------------------------------------------------------------------------
# build_batch_create_articles_query — returns plain string
# ---------------------------------------------------------------------------

def test_build_batch_create_articles_query_empty_list():
    assert queries.build_batch_create_articles_query([]) == ""


def test_build_batch_create_articles_query_single_article():
    article_data = [{'title': 'Article 1', 'namespace': 0, 'length': 100}]
    expected_query = (
        "UNWIND $props AS article_props\n"
        "CREATE (a:Article)\n"
        "SET a = article_props\n"
        "RETURN a"
    )
    assert queries.build_batch_create_articles_query(article_data) == expected_query


def test_build_batch_create_articles_query_multiple_articles():
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


# ---------------------------------------------------------------------------
# build_batch_create_links_query — returns plain string
# ---------------------------------------------------------------------------

def test_build_batch_create_links_query_empty_list():
    assert queries.build_batch_create_links_query([]) == ""


def test_build_batch_create_links_query_single_link():
    link_data = [{'from_title': 'Article A', 'to_title': 'Article B'}]
    expected_query = (
        "UNWIND $props AS link_props\n"
        "MATCH (from:Article {title: link_props.from_title})\n"
        "MATCH (to:Article {title: link_props.to_title})\n"
        "MERGE (from)-[:LINKS_TO]->(to)"
    )
    assert queries.build_batch_create_links_query(link_data) == expected_query


def test_build_batch_create_links_query_multiple_links():
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
