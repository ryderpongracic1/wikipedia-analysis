# Create a new file: wikipedia_analysis/streaming_import.py
import lxml.etree as ET
from neo4j import GraphDatabase
import sys
import time

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
    """Memory-efficient streaming import"""
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "your_password"
    print(f"Connecting to Neo4j with uri: {uri}, username: {username}, password: {password}")
    
    processed = 0
    batch_articles = []
    
    print("Starting streaming import...")
    
    try:
        # Use iterparse for streaming - doesn't load full file
        for event, elem in ET.iterparse(xml_file, events=('start', 'end')):
            print(f"Type of elem: {type(elem)}")
            print(f"Elem tag: {elem.tag}")
            if event == 'end' and elem.tag.endswith('}page'):
                # Extract article data
                ns = '{http://www.mediawiki.org/xml/export-0.11/}'
                
                id_elem = elem.find(ns + 'id')
                title_elem = elem.find(ns + 'title')
                
                if id_elem is not None and title_elem is not None:
                    article = {
                        'id': id_elem.text,
                        'title': title_elem.text,
                        'url': f"https://en.wikipedia.org/wiki/{title_elem.text.replace(' ', '_')}"
                    }
                    batch_articles.append(article)
                
                # Critical: Clear element to free memory immediately
                elem.clear()
                
                # Process batch
                if len(batch_articles) >= batch_size:
                    import_batch(batch_articles, uri, username, password)
                    processed += len(batch_articles)
                    print(f"✓ Processed {processed} articles...")
                    batch_articles.clear()
                    
                    # Check memory pressure every 1000 articles
                    if processed % 1000 == 0:
                        if not check_memory_pressure():
                            print("⚠️  High memory pressure detected - pausing...")
                            time.sleep(5)
        
        # Process remaining articles
        if batch_articles:
            import_batch(batch_articles, uri, username, password)
            processed += len(batch_articles)
        
        print(f"🎉 Import complete! Total: {processed} articles")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True

def import_batch(articles, uri, username, password):
    """Import batch to Neo4j"""
    try:
        with GraphDatabase.driver(uri, auth=(username, password)) as driver:
            with driver.session() as session:
                session.run("""
                    UNWIND $articles AS article
                    MERGE (a:Article {id: article.id})
                    SET a.title = article.title, a.url = article.url
                """, articles=articles)
    except Exception as e:
        print(f"❌ Neo4j connection error: {e}")


import os

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
