import sys
import time
import os
from wikipedia_analysis.data_processing import parse_dump_file, batch_data, transform_to_article_node
from wikipedia_analysis.database import Neo4jConnectionManager, batch_import_nodes

def check_memory_pressure():
    """Simple memory check before processing"""
    import subprocess
    try:
        result = subprocess.run(['memory_pressure'], capture_output=True, text=True)
        memory_pressure_status = "normal" in result.stdout.lower()
        print(f"Memory pressure status: {memory_pressure_status}")
        return memory_pressure_status
    except Exception as e:
        print(f"Error checking memory pressure: {e}")
        return True  # Assume OK if can't check

def streaming_import(xml_file, batch_size=100):
    """Memory-efficient streaming import using data_processing module."""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "my_password")
    
    print(f"Connecting to Neo4j with uri: {uri}, username: {username}")
    
    processed_articles = 0
    
    print("Starting streaming import...")
    
    try:
        article_data_iterator = parse_dump_file(xml_file)
        
        with Neo4jConnectionManager(uri, username, password) as driver_manager:
            driver = driver_manager.get_driver()
            if not driver:
                print("Failed to get Neo4j driver. Exiting.", file=sys.stderr)
                return False

            with driver.session() as session:
                for batch in batch_data(article_data_iterator, batch_size):
                    # Transform raw parsed data into Neo4j node format
                    transformed_articles = [transform_to_article_node(article) for article in batch if article]
                    
                    if transformed_articles:
                        # Use batch_import_nodes from database.py
                        batch_import_nodes(session, "Article", transformed_articles)
                        processed_articles += len(transformed_articles)
                        print(f"✓ Processed {processed_articles} articles...")
                        
                        # Check memory pressure every 1000 articles
                        if processed_articles % 1000 == 0:
                            if not check_memory_pressure():
                                print("⚠️  High memory pressure detected - pausing...")
                                time.sleep(5)
        
        print(f"🎉 Import complete! Total: {processed_articles} articles")
        
    except Exception as e:
        print(f"❌ Error during streaming import: {e}")
        return False
    
    return True

if __name__ == "__main__":
    xml_file = "wikipedia_analysis/pages-articles.xml"
    
    if not os.path.exists(xml_file):
        print(f"❌ Error: XML file not found at {xml_file}")
        success = False
    else:
        print(f"✅ XML file found at {xml_file}")
        success = streaming_import(xml_file, batch_size=50)  # Smaller batches
    
    if success:
        print("Import completed successfully!")
    else:
        print("Import failed - check error messages above")
