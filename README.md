Wikipedia to Neo4j Importer & Citation Network Analysis
This project provides a set of Python scripts to parse the official Wikipedia XML dump (pages-articles.xml), import the articles and their internal links into a Neo4j graph database, and perform network analysis on the resulting graph.

The project is divided into three phases:

Node Import: A fast import of just the article nodes (titles and IDs).

Link Import: A much slower, more detailed import that also creates [:LINKS_TO] relationships between articles.

Network Analysis: Scripts to analyze the citation network to find influential articles and knowledge paths.

Prerequisites
macOS: With Homebrew.

Python 3: And the neo4j library (pip3 install neo4j).

Neo4j Desktop or Server: The graph database.

(Recommended) Neo4j Graph Data Science (GDS) Library: Required for the PageRank "influence score" analysis. See the GDS Installation Guide.

Setup & Execution
Step 1: Download Wikipedia Data
Download and decompress the pages-articles.xml.bz2 dump from the Wikipedia Dump Service. Place the final .xml file inside the wikipedia_analysis directory.

Step 2: Configure Scripts
In both import_with_links.py and run_analysis.py, update the password variable to match your Neo4j database password.

Step 3: Start the Neo4j Database
Open a terminal and run Neo4j in console mode. Leave this terminal running.

neo4j console

Phase 1: Import Links (The Main Import)
This script will populate your database with both the articles and the citation links between them. This will take many hours.

Open a new terminal window.

Navigate to the project folder.

Run the script using caffeinate to prevent your computer from sleeping:

caffeinate python wikipedia_analysis/import_with_links.py

You will see it print the title of each article and the number of links it found as it works.

Phase 2: Run the Network Analysis
After the import is complete, you can run the analysis script to get insights from your graph.

Make sure your Neo4j database is still running.

In a terminal, run the script:

python wikipedia_analysis/run_analysis.py

The script will run several queries and print the results to your console, including:

The top 20 most-cited articles.

The top 20 most influential articles (based on PageRank score).

The shortest "knowledge path" between example topics like "Graph theory" and "Social network".

## Importer Usage

The `wikipedia_analysis/import_with_links.py` script now includes category-parsing functionality.

To run the importer:

```bash
python wikipedia_analysis/import_with_links.py [filename]
```

-   **`[filename]` (optional):** Specify the path to a Wikipedia XML dump file (e.g., `wikipedia_analysis/sample-articles.xml`).
-   If no filename is provided, the script will default to looking for `wikipedia_analysis/pages-articles.xml`.

## API Usage

A Flask API server (`wikipedia_analysis/api.py`) is available to query the imported data.

To start the API server:

```bash
python wikipedia_analysis/api.0py
```

The API will run on `http://127.0.0.1:5000/`.

Available Endpoints:

-   **`GET /categories`**: Returns a list of all unique categories found in the imported data.
    Example `curl` command:
    ```bash
    curl http://127.0.0.1:5000/categories
    ```

-   **`GET /category/<category_name>`**: Returns a list of articles belonging to the specified category. Replace `<category_name>` with the actual category name (e.g., "Graph theory").
    Example `curl` command:
    ```bash
    curl http://127.0.0.1:5000/category/Graph%20theory