import xml.etree.ElementTree as ET
from neo4j import GraphDatabase
import sys
import re
from wikipedia_analysis.database import create_constraints_and_indexes

# --- Configuration ---
# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
# IMPORTANT: Replace "your_password" with your actual Neo4j password
password = "my_password"

# XML file path
xml_file = "wikipedia_analysis/pages-articles.xml"

# --- Neo4j Functions ---
def create_article_and_links(tx, article_id, title, links, categories):
    """
    Creates the source :Article node and merges relationships to target articles.
    This is designed to be highly efficient by processing links in a batch.
    """
    # 1. First, create or find the source article node.
    # We use MERGE to avoid creating duplicate nodes on reruns.
    query_source = """
    MERGE (a:Article {id: $article_id})
    SET a.title = $title
    """
    tx.run(query_source, article_id=article_id, title=title)

    # 2. If there are links, create relationships.
    if links:
        # Use UNWIND to process a list of links in a single, fast operation.
        # For each link title in our list, we find or create the target node
        # and then create the LINKS_TO relationship.
        query_links = """
        MATCH (source:Article {id: $article_id})
        UNWIND $links as target_title
        MERGE (target:Article {title: target_title})
        MERGE (source)-[:LINKS_TO]->(target)
        """
        tx.run(query_links, article_id=article_id, links=links)

    # 3. Create or find category nodes and establish relationships
    if categories:
        query_categories = """
        MATCH (source:Article {id: $article_id})
        UNWIND $categories as category_name
        MERGE (category:Category {name: category_name})
        MERGE (source)-[:IN_CATEGORY]->(category)
        """
        tx.run(query_categories, article_id=article_id, categories=categories)

# --- Main Parsing Logic ---
def parse_wikitext_and_import(xml_file_path):
    """
    Parses the Wikipedia XML, extracts article text and internal links,
    and imports the structure into Neo4j.
    """
    # The namespace for MediaWiki XML elements
    ns = '{http://www.mediawiki.org/xml/export-0.10/}'
    
    print("Connecting to Neo4j...")
    try:
        with GraphDatabase.driver(uri, auth=(username, password)) as driver:
            driver.verify_connectivity()
            print("Connection successful.")
            
            with driver.session() as session:
                create_constraints_and_indexes(session)
                print("Ensured Neo4j constraints and indexes are in place.")
                print(f"Starting to parse and import links from {xml_file_path}...")
                print("NOTE: This process will be significantly slower than the first import.")

                # Use iterparse for memory-efficient, event-based parsing.
                context = ET.iterparse(xml_file_path, events=('end',))
                
                for event, elem in context:
                    # When a 'page' element is fully parsed, process it.
                    if elem.tag == ns + 'page':
                        try:
                            article_id = elem.find(ns + 'id').text
                            title = elem.find(ns + 'title').text
                            
                            # Find the article text, which is in the 'revision/text' element
                            wikitext_element = elem.find(f'.//{ns}text')
                            wikitext = wikitext_element.text if wikitext_element is not None else ""

                            if not article_id or not title:
                                continue

                            # Use regex to find all [[Internal Link]] patterns.
                            # This is a simplified pattern; wikitext is complex.
                            raw_links = re.findall(r'\[\[(.*?)\]\]', wikitext or "")
                            
                            # Extract categories
                            categories = set()
                            category_matches = re.findall(r'\[\[Category:(.*?)(?:\|.*?)?\]\]', wikitext or "")
                            for category in category_matches:
                                categories.add(category.strip())

                            # Clean the links: remove pipe tricks, section links, and file/category links.
                            cleaned_links = set()
                            for link in raw_links:
                                if any(prefix in link for prefix in ['File:', 'Category:', 'Image:', 'Template:']):
                                    continue
                                # Remove display text (e.g., [[Target|Display Text]])
                                clean_link = link.split('|')[0]
                                # Remove section links (e.g., [[Target#Section]])
                                clean_link = clean_link.split('#')[0]
                                clean_link = clean_link.strip()
                                if clean_link:
                                    cleaned_links.add(clean_link)
                            
                            # Write to Neo4j in a transaction
                            session.write_transaction(create_article_and_links, article_id, title, list(cleaned_links), list(categories))
                            
                            # Print progress
                            print(f"Imported: '{title}' with {len(cleaned_links)} links and {len(categories)} categories.")

                        except AttributeError as e:
                            print(f"Skipping page due to parsing error: {e}", file=sys.stderr)
                        
                        # CRITICAL: Clear the element to free memory.
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                
                del context
                print("\nFinished importing all articles and links.")

    except ET.ParseError as e:
        print(f"\nFATAL: XML Parsing Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"\nFATAL: An unexpected error occurred: {e}", file=sys.stderr)

# --- Script Execution ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_xml_file = sys.argv[1]
    else:
        input_xml_file = xml_file # Default from configuration
    
    parse_wikitext_and_import(input_xml_file)
