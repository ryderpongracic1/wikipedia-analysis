import json
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def client():
    from wikipedia_analysis.api import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _mock_session(records):
    """Return a context-manager mock whose session.run() yields the given records."""
    mock_session = MagicMock()
    mock_session.run.return_value = iter(records)
    mock_session.__enter__ = lambda s: s
    mock_session.__exit__ = MagicMock(return_value=False)
    return mock_session


# ---------------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------------

def test_index(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Wikipedia Analysis API" in resp.data


# ---------------------------------------------------------------------------
# GET /categories
# ---------------------------------------------------------------------------

def test_get_categories_success(client):
    records = [{"categoryName": "Graph theory"}, {"categoryName": "Computer science"}]
    mock_sess = _mock_session(records)
    with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
        resp = client.get("/categories")
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data == ["Graph theory", "Computer science"]


def test_get_categories_empty(client):
    mock_sess = _mock_session([])
    with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
        resp = client.get("/categories")
    assert resp.status_code == 200
    assert json.loads(resp.data) == []


def test_get_categories_db_error(client):
    mock_sess = MagicMock()
    mock_sess.__enter__ = lambda s: s
    mock_sess.__exit__ = MagicMock(return_value=False)
    mock_sess.run.side_effect = Exception("connection refused")
    with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
        resp = client.get("/categories")
    assert resp.status_code == 500
    assert "error" in json.loads(resp.data)


# ---------------------------------------------------------------------------
# GET /category/<category_name>
# ---------------------------------------------------------------------------

def test_get_articles_in_category_success(client):
    records = [{"articleTitle": "Graph theory"}, {"articleTitle": "Dijkstra's algorithm"}]
    mock_sess = _mock_session(records)
    with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
        resp = client.get("/category/Computer%20science")
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data == ["Graph theory", "Dijkstra's algorithm"]


def test_get_articles_in_category_empty_result(client):
    mock_sess = _mock_session([])
    with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
        resp = client.get("/category/Unknown%20Category")
    assert resp.status_code == 200
    assert json.loads(resp.data) == []


def test_get_articles_in_category_whitespace_only(client):
    # Flask routes %20 as a space character; strip() should catch it
    resp = client.get("/category/%20")
    assert resp.status_code == 400
    assert "error" in json.loads(resp.data)


def test_get_articles_in_category_db_error(client):
    mock_sess = MagicMock()
    mock_sess.__enter__ = lambda s: s
    mock_sess.__exit__ = MagicMock(return_value=False)
    mock_sess.run.side_effect = Exception("query failed")
    with patch("wikipedia_analysis.api.get_db_session", return_value=mock_sess):
        resp = client.get("/category/Physics")
    assert resp.status_code == 500
    assert "error" in json.loads(resp.data)
