import xml.etree.ElementTree as ET
from neo4j import GraphDatabase
import sys

# --- Configuration ---
# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
# IMPORTANT: Replace "your_password" with your actual Neo4j password
password = "my_password"

# XML file path
xml_file = "wikipedia_analysis/pages-articles.xml"

# --- Neo4j Functions ---
def create_article_node(tx, article_id, title, url):
    """Creates an :Article node in Neo4j."""
    query = """
    MERGE (a:Article {id: $article_id})
    SET a.title = $title, a.url = $url
    """
    tx.run(query, article_id=article_id, title=title, url=url)

# This function is not used in this script but is kept for future use
def create_citation_relationship(tx, source_id, target_id):
    """Creates a [:CITES] relationship between two articles."""
    query = """
    MATCH (a:Article {id: $source_id})
    MATCH (b:Article {id: $target_id})
    MERGE (a)-[:CITES]->(b)
    """
    tx.run(query, source_id=source_id, target_id=target_id)

# --- Main Parsing Logic ---
def parse_xml_and_import_to_neo4j(xml_file_path):
    """
    Parses a large XML file iteratively and imports data into Neo4j
    to avoid high memory consumption.
    """
    # The namespace for MediaWiki XML elements
    ns = '{http://www.mediawiki.org/xml/export-0.10/}'
    
    print("Connecting to Neo4j...")
    try:
        with GraphDatabase.driver(uri, auth=(username, password)) as driver:
            driver.verify_connectivity()
            print("Connection successful.")
            
            with driver.session() as session:
                print(f"Starting to parse and import {xml_file_path}...")
                # Use iterparse for memory-efficient, event-based parsing.
                # We only care about the 'end' event for each element.
                context = ET.iterparse(xml_file_path, events=('end',))
                
                for event, elem in context:
                    # When a 'page' element is fully parsed, process it.
                    if elem.tag == ns + 'page':
                        try:
                            article_id = elem.find(ns + 'id').text
                            title = elem.find(ns + 'title').text
                            # Skip pages without an ID or title
                            if not article_id or not title:
                                continue

                            url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                            
                            # Write to Neo4j in a transaction
                            session.write_transaction(create_article_node, article_id, title, url)
                            # Print progress to the console
                            print(f"Imported: {title}")

                        except AttributeError as e:
                            # This can happen if a page is missing an expected tag
                            print(f"Skipping page due to parsing error: {e}", file=sys.stderr)
                        
                        # CRITICAL: Clear the element and its ancestors to free memory.
                        # This is the key to processing large files.
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                
                # Clean up the context iterator
                del context
                print("\nFinished importing all articles.")

    except ET.ParseError as e:
        print(f"\nFATAL: XML Parsing Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"\nFATAL: An unexpected error occurred: {e}", file=sys.stderr)

# --- Script Execution ---
if __name__ == "__main__":
    parse_xml_and_import_to_neo4j(xml_file)