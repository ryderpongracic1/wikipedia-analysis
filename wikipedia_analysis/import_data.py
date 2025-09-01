import xml.etree.ElementTree as ET
import sys
import os
from wikipedia_analysis.database import Neo4jConnectionManager, create_article_node

# --- Configuration ---
# Neo4j connection details
uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USERNAME", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "my_password") # Consider using a more secure way to handle passwords

# XML file path
xml_file = "wikipedia_analysis/pages-articles.xml"

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
        with Neo4jConnectionManager(uri, username, password) as driver_manager:
            driver = driver_manager.get_driver()
            if not driver:
                print("Failed to get Neo4j driver. Exiting.", file=sys.stderr)
                return

            with driver.session() as session:
                print("Connection successful.")
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

                            # Assuming 'url' is not directly in the XML, construct it or remove if not needed
                            # For now, let's pass a placeholder or remove if create_article_node doesn't need it
                            # The database.py create_article_node expects id, title, namespace, redirect_title, is_redirect
                            # We need to adapt this. For now, let's just pass what we have and add defaults.
                            article_data = {
                                "id": article_id,
                                "title": title,
                                "namespace": "0", # Default namespace for articles
                                "redirect_title": None,
                                "is_redirect": False
                            }
                            
                            # Write to Neo4j in a transaction
                            session.write_transaction(create_article_node, article_data)
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