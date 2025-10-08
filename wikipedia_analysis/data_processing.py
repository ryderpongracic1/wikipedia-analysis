import lxml.etree as ET
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_title(title):
    """Cleans a Wikipedia article title."""
    if not title:
        return ""
    title = title.strip()
    title = re.sub(r'\s+', ' ', title)
    return title

def validate_length(data, max_length=None):
    """Validates the length of a string or the size of a collection."""
    if data is None:
        return False
    if max_length is not None and len(data) > max_length:
        return False
    return True

def parse_dump_file(xml_file_path):
    """
    Parses a Wikipedia XML dump file and yields article data.
    Extracts article ID, title, and links.

    Robust parsing strategy:
    - Read via builtin open so tests that patch builtins.open work.
    - Attempt streaming parse with lxml.iterparse (fast).
    - If streaming parse raises XMLSyntaxError (malformed document), fall back
      to extracting <page>...</page> fragments and parsing those individually,
      which lets us skip malformed pages while returning valid ones.
    """
    from io import BytesIO

    def _local_name(tag: str) -> str:
        return tag.split('}')[-1] if '}' in tag else tag

    def _find_child_by_localname(parent, name: str):
        if parent is None:
            return None
        for child in parent:
            if _local_name(child.tag) == name:
                return child
        for child in parent.iter():
            if _local_name(child.tag) == name:
                return child
        return None

    try:
        with open(xml_file_path, 'r', encoding='utf-8') as fh:
            content = fh.read()
    except FileNotFoundError:
        raise

    # Try streaming parse first. Track which pages we've yielded so fallback won't duplicate.
    seen_ids = set()
    try:
        xml_bytes = BytesIO(content.encode('utf-8'))
        for event, elem in ET.iterparse(xml_bytes, events=('end',)):
            if _local_name(elem.tag) != 'page':
                continue
            try:
                id_elem = _find_child_by_localname(elem, 'id')
                title_elem = _find_child_by_localname(elem, 'title')
                revision_elem = _find_child_by_localname(elem, 'revision')
                text_elem = _find_child_by_localname(revision_elem, 'text') if revision_elem is not None else None

                if id_elem is not None and title_elem is not None:
                    article_data = {}
                    article_data['id'] = id_elem.text
                    article_data['title'] = clean_title(title_elem.text)
                    article_data['url'] = f"https://en.wikipedia.org/wiki/{article_data['title'].replace(' ', '_')}"

                    links = []
                    if text_elem is not None and text_elem.text:
                        link_pattern = re.compile(r'\[\[([^|\]]+)(?:\|[^\]]+)?\]\]')
                        for match in link_pattern.finditer(text_elem.text):
                            link_title = clean_title(match.group(1))
                            if link_title and link_title != article_data['title']:
                                links.append(link_title)
                    article_data['links'] = list(set(links))
                    seen_ids.add(article_data['id'])
                    yield article_data
            finally:
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
        return
    except ET.XMLSyntaxError as e:
        # Log the streaming parse error and fall back to fragment parsing.
        logging.getLogger(__name__).error("XMLSyntaxError during streaming parse: %s", e)
        # Fall back to fragment parsing
        pass

    # Fallback: extract page fragments and parse individually
    page_fragments = re.findall(r"<page.*?>.*?</page>", content, flags=re.DOTALL)
    for frag in page_fragments:
        try:
            page_elem = ET.fromstring(frag)
            id_elem = _find_child_by_localname(page_elem, 'id')
            title_elem = _find_child_by_localname(page_elem, 'title')
            revision_elem = _find_child_by_localname(page_elem, 'revision')
            text_elem = _find_child_by_localname(revision_elem, 'text') if revision_elem is not None else None

            if id_elem is None or title_elem is None:
                continue

            # Skip pages already yielded by streaming parse to avoid duplicates
            frag_id = id_elem.text
            if frag_id in seen_ids:
                continue

            article_data = {}
            article_data['id'] = frag_id
            article_data['title'] = clean_title(title_elem.text)
            article_data['url'] = f"https://en.wikipedia.org/wiki/{article_data['title'].replace(' ', '_')}"
            links = []
            if text_elem is not None and text_elem.text:
                link_pattern = re.compile(r'\[\[([^|\]]+)(?:\|[^\]]+)?\]\]')
                for match in link_pattern.finditer(text_elem.text):
                    link_title = clean_title(match.group(1))
                    if link_title and link_title != article_data['title']:
                        links.append(link_title)
            article_data['links'] = list(set(links))
            seen_ids.add(article_data['id'])
            yield article_data
        except ET.XMLSyntaxError as e:
            # Log the fragment that failed to parse so tests can assert on log contents
            logging.getLogger(__name__).error("Error parsing page fragment: %s", frag)
            continue

def batch_data(data_iterator, batch_size):
    """Batches data from an iterator into lists of a specified size."""
    batch = []
    for item in data_iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def transform_to_article_node(article_data):
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

def transform_to_category_node(category_data):
    """Transforms parsed category data into a Neo4j Category node format."""
    if not category_data or 'title' not in category_data:
        logging.warning(f"Invalid category data for transformation: {category_data}")
        return None
    return {
        'title': category_data['title'],
        'depth': category_data.get('depth', 0)
    }

def transform_to_links_to_relationship(source_article_id, target_article_title):
    """Transforms data into a LINKS_TO relationship format."""
    if not source_article_id or not target_article_title:
        logging.warning(f"Invalid data for LINKS_TO relationship: source_id={source_article_id}, target_title={target_article_title}")
        return None
    return {
        'source_id': source_article_id,
        'target_title': target_article_title
    }

def transform_to_belongs_to_relationship(article_id, category_title):
    """Transforms data into a BELONGS_TO relationship format."""
    if not article_id or not category_title:
        logging.warning(f"Invalid data for BELONGS_TO relationship: article_id={article_id}, category_title={category_title}")
        return None
    return {
        'article_id': article_id,
        'category_title': category_title
    }

def transform_to_redirects_to_relationship(source_article_id, target_article_title):
    """Transforms data into a REDIRECTS_TO relationship format."""
    if not source_article_id or not target_article_title:
        logging.warning(f"Invalid data for REDIRECTS_TO relationship: source_id={source_article_id}, target_title={target_article_title}")
        return None
    return {
        'source_id': source_article_id,
        'target_title': target_article_title
    }