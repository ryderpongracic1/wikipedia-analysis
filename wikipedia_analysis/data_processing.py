import lxml.etree as ET
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_title(title):
    """Cleans a Wikipedia article title."""
    if not title:
        return ""
    # Remove leading/trailing whitespace
    title = title.strip()
    # Replace multiple spaces with a single space
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
    """
    ns = '{http://www.mediawiki.org/xml/export-0.11/}' # Namespace for MediaWiki XML elements
    
    for event, elem in ET.iterparse(xml_file_path, events=('end',)):
        if elem.tag == ns + 'page':
            article_data = {}
            try:
                id_elem = elem.find(ns + 'id')
                title_elem = elem.find(ns + 'title')
                text_elem = elem.find(ns + 'revision/' + ns + 'text')

                if id_elem is not None and title_elem is not None:
                    article_data['id'] = id_elem.text
                    article_data['title'] = clean_title(title_elem.text)
                    article_data['url'] = f"https://en.wikipedia.org/wiki/{article_data['title'].replace(' ', '_')}"
                    
                    # Extract links from text
                    links = []
                    if text_elem is not None and text_elem.text:
                        # Regex to find [[Article Title]] links
                        link_pattern = re.compile(r'\[\[([^|\]]+)(?:\|[^\]]+)?\]\]')
                        for match in link_pattern.finditer(text_elem.text):
                            link_title = clean_title(match.group(1))
                            if link_title and link_title != article_data['title']: # Avoid self-links
                                links.append(link_title)
                    article_data['links'] = list(set(links)) # Remove duplicates

                    yield article_data
                else:
                    logging.warning(f"Skipping page due to missing ID or title: {ET.tostring(elem, encoding='unicode')[:200]}...")

            except Exception as e:
                logging.error(f"Error parsing page: {e}. Raw element: {ET.tostring(elem, encoding='unicode')[:200]}...")
            finally:
                # Critical: Clear the element and its ancestors to free memory.
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

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
    return {
        'id': article_data['id'],
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
        'depth': category_data.get('depth', 0) # Default depth to 0 if not provided
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

# Placeholder for BELONGS_TO and REDIRECTS_TO relationships
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