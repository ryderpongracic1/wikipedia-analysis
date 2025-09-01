import pytest
from unittest.mock import mock_open, patch
import lxml.etree as ET
from wikipedia_analysis.data_processing import (
    clean_title,
    validate_length,
    parse_dump_file,
    batch_data,
    transform_to_article_node,
    transform_to_category_node,
    transform_to_links_to_relationship,
    transform_to_belongs_to_relationship,
    transform_to_redirects_to_relationship
)

# --- Fixtures for XML content ---
@pytest.fixture
def sample_xml_content():
    return """
    <mediawiki>
        <page>
            <title>Article A</title>
            <id>1</id>
            <revision>
                <text>This is [[Article B]] and [[Article C]].</text>
            </revision>
        </page>
        <page>
            <title>Article B</title>
            <id>2</id>
            <revision>
                <text>This links to [[Article A]].</text>
            </revision>
        </page>
        <page>
            <title>Category:Test</title>
            <id>3</id>
            <ns>14</ns>
            <revision>
                <text>[[Category:Main]]</text>
            </revision>
        </page>
        <page>
            <title>Redirect Page</title>
            <id>4</id>
            <revision>
                <text>#REDIRECT [[Article A]]</text>
            </revision>
        </page>
        <page>
            <title>Page with no ID</title>
            <revision>
                <text>Some text.</text>
            </revision>
        </page>
        <page>
            <id>6</id>
            <revision>
                <text>Page with no title.</text>
            </revision>
        </page>
        <page>
            <title>Empty Page</title>
            <id>7</id>
            <revision>
                <text></text>
            </revision>
        </page>
        <page>
            <title>Self Link Test</title>
            <id>8</id>
            <revision>
                <text>This page links to [[Self Link Test]].</text>
            </revision>
        </page>
    </mediawiki>
    """

@pytest.fixture
def corrupted_xml_content():
    return """
    <mediawiki>
        <page>
            <title>Valid Page</title>
            <id>10</id>
            <revision><text>Content</text></revision>
        </page>
        <page>
            <title>Malformed Page</title>
            <id>11</id>
            <revision>
                <text>Missing closing tag for revision
        </page>
    </mediawiki>
    """

# --- Test Wikipedia dump file parsing ---
def test_parse_dump_file_correctly_extracts_articles_and_links(sample_xml_content):
    m = mock_open(read_data=sample_xml_content)
    with patch('builtins.open', m):
        # For lxml.etree.iterparse, we need to mock the file-like object it receives
        # which is typically opened by 'builtins.open'.
        # We also need to mock ET.iterparse directly if it's not reading from a file path.
        # However, parse_dump_file takes a path, so mocking builtins.open is sufficient
        # as lxml.etree.iterparse will then receive the mocked file object.
        
        # Create a dummy file path for the mock
        dummy_file_path = "dummy.xml"
        
        articles = list(parse_dump_file(dummy_file_path))
        
        assert len(articles) == 5 # Article A, B, Category, Redirect, Empty Page, Self Link Test (no ID/title skipped)

        article_a = next(a for a in articles if a['id'] == '1')
        assert article_a['title'] == 'Article A'
        assert 'Article B' in article_a['links']
        assert 'Article C' in article_a['links']
        assert article_a['url'] == 'https://en.wikipedia.org/wiki/Article_A'

        article_b = next(a for a in articles if a['id'] == '2')
        assert article_b['title'] == 'Article B'
        assert 'Article A' in article_b['links']

        # Test category page (should be parsed as an article for now, links will be handled later)
        category_page = next(a for a in articles if a['id'] == '3')
        assert category_page['title'] == 'Category:Test'
        assert 'Category:Main' in category_page['links']

        # Test redirect page
        redirect_page = next(a for a in articles if a['id'] == '4')
        assert redirect_page['title'] == 'Redirect Page'
        assert 'Article A' in redirect_page['links'] # Redirects are treated as links for now

        # Test empty page
        empty_page = next(a for a in articles if a['id'] == '7')
        assert empty_page['title'] == 'Empty Page'
        assert empty_page['links'] == []

        # Test self-link is not included
        self_link_page = next(a for a in articles if a['id'] == '8')
        assert self_link_page['title'] == 'Self Link Test'
        assert self_link_page['links'] == []


def test_parse_dump_file_handles_missing_id_or_title(sample_xml_content):
    m = mock_open(read_data=sample_xml_content)
    with patch('builtins.open', m):
        dummy_file_path = "dummy.xml"
        articles = list(parse_dump_file(dummy_file_path))
        
        # Pages with no ID or no title should be skipped
        assert not any('Page with no ID' in a['title'] for a in articles if 'title' in a)
        assert not any('Page with no title' in a['title'] for a in articles if 'title' in a)
        assert len(articles) == 5 # Only valid pages should be processed

# --- Test data cleaning and validation functions ---
def test_clean_title():
    assert clean_title("  Test Article  ") == "Test Article"
    assert clean_title("Test   Article") == "Test Article"
    assert clean_title("Test-Article") == "Test-Article"
    assert clean_title("") == ""
    assert clean_title(None) == ""
    assert clean_title("  Leading/Trailing Spaces ") == "Leading/Trailing Spaces"
    assert clean_title("Multiple    Spaces   Here") == "Multiple Spaces Here"

def test_validate_length():
    assert validate_length("hello", max_length=10) == True
    assert validate_length("hello", max_length=3) == False
    assert validate_length("hello") == True # No max_length
    assert validate_length("", max_length=5) == True
    assert validate_length(None, max_length=5) == False
    assert validate_length([1, 2, 3], max_length=3) == True
    assert validate_length([1, 2, 3], max_length=2) == False

# --- Test batch processing logic ---
def test_batch_data_correctly_batches():
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    batches = list(batch_data(iter(data), 3))
    assert batches == [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]

def test_batch_data_handles_exact_batch_size():
    data = [1, 2, 3, 4, 5, 6]
    batches = list(batch_data(iter(data), 3))
    assert batches == [[1, 2, 3], [4, 5, 6]]

def test_batch_data_handles_smaller_last_batch():
    data = [1, 2, 3, 4, 5]
    batches = list(batch_data(iter(data), 3))
    assert batches == [[1, 2, 3], [4, 5]]

def test_batch_data_handles_empty_input():
    data = []
    batches = list(batch_data(iter(data), 3))
    assert batches == []

def test_batch_data_handles_batch_size_larger_than_data():
    data = [1, 2]
    batches = list(batch_data(iter(data), 5))
    assert batches == [[1, 2]]

# --- Test error handling for corrupted data ---
def test_parse_dump_file_handles_malformed_xml(corrupted_xml_content, caplog):
    m = mock_open(read_data=corrupted_xml_content)
    with patch('builtins.open', m):
        dummy_file_path = "dummy.xml"
        
        # lxml.etree.iterparse raises XMLSyntaxError for malformed XML.
        # Our parse_dump_file catches general exceptions and logs them.
        with caplog.at_level(10): # DEBUG level
            articles = list(parse_dump_file(dummy_file_path))
            
            # Expect only the valid page to be processed
            assert len(articles) == 1
            assert articles[0]['id'] == '10'
            assert articles[0]['title'] == 'Valid Page'
            
            # Check if an error was logged for the malformed page
            assert any("Error parsing page" in record.message for record in caplog.records)
            assert any("Malformed Page" in record.message for record in caplog.records)


# --- Test data transformation pipelines ---
def test_transform_to_article_node(sample_article_data):
    # Test with complete data
    article_node = transform_to_article_node(sample_article_data)
    assert article_node == {
        'id': 12345,
        'title': 'Test Article',
        'url': 'https://en.wikipedia.org/wiki/Test_Article'
    }

    # Test with missing URL (should generate from title)
    article_data_no_url = {'id': 67890, 'title': 'Another Article'}
    article_node_no_url = transform_to_article_node(article_data_no_url)
    assert article_node_no_url == {
        'id': 67890,
        'title': 'Another Article',
        'url': 'https://en.wikipedia.org/wiki/Another_Article'
    }

    # Test with missing ID or title
    assert transform_to_article_node({'title': 'No ID'}) is None
    assert transform_to_article_node({'id': 999}) is None
    assert transform_to_article_node({}) is None
    assert transform_to_article_node(None) is None

def test_transform_to_category_node(sample_category_data):
    # Test with complete data
    category_node = transform_to_category_node(sample_category_data)
    assert category_node == {
        'title': 'Test Category',
        'depth': 1
    }

    # Test with missing depth (should default to 0)
    category_data_no_depth = {'title': 'New Category'}
    category_node_no_depth = transform_to_category_node(category_data_no_depth)
    assert category_node_no_depth == {
        'title': 'New Category',
        'depth': 0
    }

    # Test with missing title
    assert transform_to_category_node({'depth': 2}) is None
    assert transform_to_category_node({}) is None
    assert transform_to_category_node(None) is None

def test_transform_to_links_to_relationship():
    rel = transform_to_links_to_relationship('1', 'Target Article')
    assert rel == {'source_id': '1', 'target_title': 'Target Article'}
    assert transform_to_links_to_relationship(None, 'Target') is None
    assert transform_to_links_to_relationship('1', None) is None

def test_transform_to_belongs_to_relationship():
    rel = transform_to_belongs_to_relationship('1', 'Test Category')
    assert rel == {'article_id': '1', 'category_title': 'Test Category'}
    assert transform_to_belongs_to_relationship(None, 'Category') is None
    assert transform_to_belongs_to_relationship('1', None) is None

def test_transform_to_redirects_to_relationship():
    rel = transform_to_redirects_to_relationship('4', 'Article A')
    assert rel == {'source_id': '4', 'target_title': 'Article A'}
    assert transform_to_redirects_to_relationship(None, 'Article') is None
    assert transform_to_redirects_to_relationship('4', None) is None