# wikipedia_analysis/data_processing.py

import lxml.etree as ET
import re
import logging
from typing import Generator, Dict, Any, Optional, List, Union
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==========================================
# Helper Functions
# ==========================================

def clean_title(title: Optional[str]) -> str:
    """
    Cleans a Wikipedia article title by stripping whitespace and normalizing spaces.
    """
    if not title:
        return ""
    title = title.strip()
    # Replace multiple whitespace characters with a single space
    title = re.sub(r'\s+', ' ', title)
    return title

def validate_length(data: Optional[Union[str, list, set, dict]], max_length: Optional[int] = None) -> bool:
    """
    Validates the length of a string or the size of a collection.
    Returns False if data is None or exceeds max_length.
    """
    if data is None:
        return False
    if max_length is not None and len(data) > max_length:
        return False
    return True

def _local_name(tag: str) -> str:
    """Extracts the local name from a namespaced XML tag."""
    return tag.split('}')[-1] if '}' in tag else tag

def _find_child_by_localname(parent: Any, name: str) -> Optional[Any]:
    """
    Finds the first child element with a specific local name, ignoring namespaces.
    Checks direct children first, then iterates if necessary.
    """
    if parent is None:
        return None
    # Check direct children first (fastest)
    for child in parent:
        if _local_name(child.tag) == name:
            return child
    # Fallback to iterating (slower, but covers cases where direct iteration might fail)
    for child in parent.iter():
        if _local_name(child.tag) == name:
            return child
    return None


# ==========================================
# Parsing & Transformation Logic
# ==========================================

# Pre-compiled once; the original recompiled this regex for every article.
_WIKILINK_RE = re.compile(r'\[\[([^|\]]+)(?:\|[^\]]+)?\]\]')

# Read the dump in bounded chunks so memory stays flat regardless of file size.
_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def _extract_article(page_elem: Any) -> Optional[Dict[str, Any]]:
    """Extract article dict (id, title, url, links) from a <page> element."""
    id_elem = _find_child_by_localname(page_elem, 'id')
    title_elem = _find_child_by_localname(page_elem, 'title')
    revision_elem = _find_child_by_localname(page_elem, 'revision')
    text_elem = _find_child_by_localname(revision_elem, 'text') if revision_elem is not None else None

    if id_elem is None or title_elem is None:
        return None

    title = clean_title(title_elem.text)
    article_data: Dict[str, Any] = {
        'id': id_elem.text,
        'title': title,
        'url': f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
    }

    links = set()
    if text_elem is not None and text_elem.text:
        for match in _WIKILINK_RE.finditer(text_elem.text):
            link_title = clean_title(match.group(1))
            if link_title and link_title != title:
                links.add(link_title)
    article_data['links'] = list(links)
    return article_data


def parse_dump_file(xml_file_path: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses a Wikipedia XML dump file and yields article data.
    Extracts article ID, title, and links.

    Robust parsing strategy:
    1. Stream the file in bounded chunks through an incremental parser, so
       memory use is flat even for the full multi-gigabyte Wikipedia dump.
    2. If the document is malformed (XMLSyntaxError), fall back to extracting
       <page>...</page> fragments and parsing those individually. Only this
       fallback path reads the whole file into memory.
    """
    # Count pages yielded by the streaming pass so the fallback will not
    # duplicate them. A counter (instead of a set of every article id) keeps
    # memory O(1) on the full ~7M-article dump: both the streaming parser and
    # the fragment regex walk pages in document order, and the document is
    # well-formed up to the error point, so the first N valid fragments are
    # exactly the N pages already yielded.
    yielded_count = 0

    # --- Strategy 1: Streaming Parse (bounded memory) ---
    parse_error: Optional[ET.XMLSyntaxError] = None
    with open(xml_file_path, 'r', encoding='utf-8') as fh:
        parser = ET.XMLPullParser(events=('end',))
        eof = False
        while not eof:
            chunk = fh.read(_CHUNK_SIZE)
            if not chunk:
                eof = True
                try:
                    parser.close()
                except ET.XMLSyntaxError as e:
                    parse_error = e
            else:
                data = chunk.encode('utf-8') if isinstance(chunk, str) else chunk
                try:
                    parser.feed(data)
                except ET.XMLSyntaxError as e:
                    parse_error = e

            # Drain any events produced before a potential error.
            for _event, elem in parser.read_events():
                if _local_name(elem.tag) != 'page':
                    continue
                try:
                    article_data = _extract_article(elem)
                    if article_data is not None:
                        yielded_count += 1
                        yield article_data
                finally:
                    # Clear processed subtree to keep memory bounded.
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

            if parse_error is not None:
                break

    if parse_error is None:
        return

    logging.getLogger(__name__).error(
        "XMLSyntaxError during streaming parse: %s", parse_error
    )

    # --- Strategy 2: Fallback Fragment Parsing (malformed documents only) ---
    with open(xml_file_path, 'r', encoding='utf-8') as fh:
        content = fh.read()
    page_fragments = re.findall(r"<page.*?>.*?</page>", content, flags=re.DOTALL)
    valid_fragments_seen = 0
    for frag in page_fragments:
        try:
            page_elem = ET.fromstring(frag)
            article_data = _extract_article(page_elem)
            if article_data is None:
                continue
            valid_fragments_seen += 1
            if valid_fragments_seen <= yielded_count:
                # Already yielded by the streaming pass.
                continue
            yield article_data
        except ET.XMLSyntaxError:
            # Log the fragment that failed to parse so tests can assert on log contents
            logging.getLogger(__name__).error("Error parsing page fragment: %s", frag)
            continue

def batch_data(data_iterator: Generator[Any, None, None], batch_size: int) -> Generator[List[Any], None, None]:
    """Batches data from an iterator into lists of a specified size."""
    batch = []
    for item in data_iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def transform_to_article_node(article_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Transforms parsed article data into a Neo4j Article node format."""
    if not article_data or 'id' not in article_data or 'title' not in article_data:
        logging.warning(f"Invalid article data for transformation: {article_data}")
        return None
    
    article_id = article_data['id']
    try:
        article_id = int(article_id)
    except (TypeError, ValueError):
        pass
        
    return {
        'id': article_id,
        'title': article_data['title'],
        'url': article_data.get('url', f"https://en.wikipedia.org/wiki/{article_data['title'].replace(' ', '_')}")
    }

def transform_to_category_node(category_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Transforms parsed category data into a Neo4j Category node format."""
    if not category_data or 'title' not in category_data:
        logging.warning(f"Invalid category data for transformation: {category_data}")
        return None
    return {
        'title': category_data['title'],
        'depth': category_data.get('depth', 0)
    }

def transform_to_links_to_relationship(source_article_id: Any, target_article_title: str) -> Optional[Dict[str, Any]]:
    """Transforms data into a LINKS_TO relationship format."""
    if not source_article_id or not target_article_title:
        logging.warning(f"Invalid data for LINKS_TO relationship: source_id={source_article_id}, target_title={target_article_title}")
        return None
    return {
        'source_id': source_article_id,
        'target_title': target_article_title
    }

def transform_to_belongs_to_relationship(article_id: Any, category_title: str) -> Optional[Dict[str, Any]]:
    """Transforms data into a BELONGS_TO relationship format."""
    if not article_id or not category_title:
        logging.warning(f"Invalid data for BELONGS_TO relationship: article_id={article_id}, category_title={category_title}")
        return None
    return {
        'article_id': article_id,
        'category_title': category_title
    }

def transform_to_redirects_to_relationship(source_article_id: Any, target_article_title: str) -> Optional[Dict[str, Any]]:
    """Transforms data into a REDIRECTS_TO relationship format."""
    if not source_article_id or not target_article_title:
        logging.warning(f"Invalid data for REDIRECTS_TO relationship: source_id={source_article_id}, target_title={target_article_title}")
        return None
    return {
        'source_id': source_article_id,
        'target_title': target_article_title
    }
