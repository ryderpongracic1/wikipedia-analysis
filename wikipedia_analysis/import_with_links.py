"""Import Wikipedia articles, links, and categories into Neo4j.

Fixes over the original implementation:

- **Single node identity.** Every ``Article`` node is merged on ``title``
  (the key that wiki links actually reference), with the numeric page ``id``
  stored as a property. The original merged sources on ``id`` but link
  targets on ``title``, which split most articles into two disconnected
  nodes and silently broke PageRank, in-degree, and path queries.
- **Namespace-agnostic parsing.** Works with any MediaWiki export version
  (0.10, 0.11, ...) instead of a hardcoded 0.10 namespace that matched
  nothing on current dumps.
- **Streaming with stdlib ElementTree.** The original called the lxml-only
  ``getprevious()/getparent()`` API on stdlib elements and crashed after the
  first page. Memory is now reclaimed by clearing the root element, which
  works on both stdlib and lxml.
- **Batched transactions.** Pages are imported ``BATCH_SIZE`` at a time with
  ``UNWIND`` instead of one transaction (three round-trips) per article.
- **Configuration from the environment** via ``load_neo4j_config`` — no
  hardcoded password.
"""

import re
import sys
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterator, List

if __package__ in (None, ""):  # allow `python wikipedia_analysis/import_with_links.py`
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neo4j import GraphDatabase

from wikipedia_analysis.config import load_neo4j_config

# Default XML file path (override with the first CLI argument)
DEFAULT_XML_FILE = "wikipedia_analysis/pages-articles.xml"
BATCH_SIZE = 200

_LINK_RE = re.compile(r"\[\[(.*?)\]\]")
_CATEGORY_RE = re.compile(r"\[\[Category:(.*?)(?:\|.*?)?\]\]")
_SKIP_PREFIXES = ("File:", "Category:", "Image:", "Template:")


def _local_name(tag: str) -> str:
    """Strip any XML namespace: '{...ns...}page' -> 'page'."""
    return tag.rsplit("}", 1)[-1]


def _find_by_local_name(elem: Any, name: str) -> Any:
    for child in elem:
        if _local_name(child.tag) == name:
            return child
    return None


def extract_links_and_categories(wikitext: str) -> (List[str], List[str]):
    """Extract cleaned internal link targets and category names from wikitext."""
    links = set()
    for raw in _LINK_RE.findall(wikitext or ""):
        if any(raw.startswith(prefix) or prefix in raw.split("|")[0] for prefix in _SKIP_PREFIXES):
            continue
        target = raw.split("|")[0].split("#")[0].strip()
        if target:
            links.add(target)
    categories = {c.strip() for c in _CATEGORY_RE.findall(wikitext or "") if c.strip()}
    return sorted(links), sorted(categories)


def iter_pages(xml_file_path: str) -> Iterator[Dict[str, Any]]:
    """Stream pages from a MediaWiki dump of any export version.

    Yields dicts with keys ``id``, ``title``, ``links``, ``categories``.
    Memory stays bounded: processed subtrees are dropped from the root.
    """
    context = ET.iterparse(xml_file_path, events=("start", "end"))
    _, root = next(context)  # grab the root element from the first start event
    for event, elem in context:
        if event != "end" or _local_name(elem.tag) != "page":
            continue
        try:
            id_elem = _find_by_local_name(elem, "id")
            title_elem = _find_by_local_name(elem, "title")
            revision = _find_by_local_name(elem, "revision")
            text_elem = _find_by_local_name(revision, "text") if revision is not None else None

            article_id = id_elem.text if id_elem is not None else None
            title = title_elem.text.strip() if title_elem is not None and title_elem.text else None
            if not article_id or not title:
                continue

            links, categories = extract_links_and_categories(
                text_elem.text if text_elem is not None else ""
            )
            yield {"id": article_id, "title": title, "links": links, "categories": categories}
        finally:
            # Works on both stdlib ElementTree and lxml: drop the processed
            # subtree, then prune completed children off the root.
            elem.clear()
            root.clear()


def import_page_batch(tx, pages: List[Dict[str, Any]]) -> None:
    """Import a batch of pages in one transaction.

    All Article nodes are merged on ``title`` so a page and any earlier link
    references to it resolve to the same node.
    """
    tx.run(
        """
        UNWIND $pages AS page
        MERGE (a:Article {title: page.title})
        SET a.id = page.id
        """,
        pages=pages,
    )
    tx.run(
        """
        UNWIND $pages AS page
        MATCH (source:Article {title: page.title})
        UNWIND page.links AS target_title
        MERGE (target:Article {title: target_title})
        MERGE (source)-[:LINKS_TO]->(target)
        """,
        pages=[p for p in pages if p["links"]],
    )
    tx.run(
        """
        UNWIND $pages AS page
        MATCH (source:Article {title: page.title})
        UNWIND page.categories AS category_name
        MERGE (category:Category {name: category_name})
        MERGE (source)-[:BELONGS_TO]->(category)
        """,
        pages=[p for p in pages if p["categories"]],
    )


def create_import_constraints(session) -> None:
    """Constraints matching the title-keyed import model.

    Databases created by the previous schema carry a plain index on
    Article.title, which blocks constraint creation; in that case we warn and
    continue (MERGE still works, uniqueness is just not enforced by the DB).
    """
    for query in (
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Article) REQUIRE a.title IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.id)",
    ):
        try:
            session.run(query)
        except Exception as e:
            if "IndexAlreadyExists" in str(e) or "EquivalentSchemaRuleAlreadyExists" in str(e):
                print(f"Note: skipping schema statement (legacy index present): {e}", file=sys.stderr)
            else:
                raise


def parse_wikitext_and_import(xml_file_path: str, batch_size: int = BATCH_SIZE) -> int:
    """Parse the dump and import into Neo4j. Returns number of pages imported."""
    cfg = load_neo4j_config()
    print("Connecting to Neo4j...")
    imported = 0
    with GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password)) as driver:
        driver.verify_connectivity()
        print("Connection successful.")
        with driver.session() as session:
            create_import_constraints(session)
            print(f"Starting to parse and import links from {xml_file_path}...")
            batch: List[Dict[str, Any]] = []
            for page in iter_pages(xml_file_path):
                batch.append(page)
                if len(batch) >= batch_size:
                    session.execute_write(import_page_batch, batch)
                    imported += len(batch)
                    print(f"Imported {imported} pages (latest: '{batch[-1]['title']}')")
                    batch = []
            if batch:
                session.execute_write(import_page_batch, batch)
                imported += len(batch)
            print(f"\nFinished importing {imported} pages.")
    return imported


def main() -> None:
    xml_file_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_XML_FILE
    try:
        parse_wikitext_and_import(xml_file_path)
    except ET.ParseError as e:
        print(f"\nFATAL: XML Parsing Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL: An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
