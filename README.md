Wikipedia Citation Network Analysis
A comprehensive analysis of Wikipedia's internal citation network using graph database technology to uncover knowledge patterns, influential articles, and information flow dynamics.

Overview
This project analyzes the massive network of internal links within Wikipedia to understand how knowledge is structured and connected. Using Neo4j as our graph database, we explore relationships between approximately 7 million English Wikipedia articles and over 1.2 billion internal links.

Features
Network Analysis: Identify the most influential articles and knowledge hubs.

Shortest Path Finding: Discover the degrees of separation between any two Wikipedia topics.

Community Detection: Find clusters of related articles and topics.

Citation Flow Analysis: Track how information propagates through Wikipedia.

Temporal Analysis: Study how the citation network evolves over time.

Database System Selection
After careful consideration, Neo4j has been selected as the database system for this project.

Reasons for Choosing Neo4j
Graph Database: Neo4j is purpose-built for analyzing relationships between data points, making it ideal for Wikipedia's citation network.

Scalability: Handles large datasets efficiently (7M+ articles, 1.2B+ links).

Query Performance: Optimized for graph traversal operations like shortest paths and centrality calculations.

Cypher Query Language: Intuitive and powerful query language for complex graph operations.

Mature Ecosystem: Extensive documentation, community support, and visualization tools.

Strengths
Excellent performance for graph-based queries (shortest paths, centrality measures).

Native graph storage and processing.

Rich visualization capabilities.

ACID compliance and enterprise features.

Weaknesses
Higher memory requirements for large datasets.

Licensing costs for enterprise features.

Learning curve for Cypher query language.

Alternatives Considered
Amazon Neptune: Fully managed service with good scalability, but less mature tooling ecosystem.

PostgreSQL with graph extensions: Cost-effective for mixed workloads, but suboptimal performance for pure graph operations.

Apache TinkerPop/JanusGraph: Open-source alternative, but requires more infrastructure management.

Database Schema
Nodes
Article
Represents a Wikipedia article with the following properties:

Cypher

CREATE (a:Article {
  id: INTEGER,           // Unique Wikipedia page ID
  title: STRING,         // Article title
  namespace: INTEGER,    // Wikipedia namespace (0 for main articles)
  length: INTEGER,       // Article length in bytes
  created_date: DATE,    // Article creation date
  last_modified: DATE    // Last modification date
})
Category
Represents Wikipedia categories:

Cypher

CREATE (c:Category {
  id: INTEGER,           // Unique category ID
  name: STRING,          // Category name
  subcategory_count: INTEGER,
  article_count: INTEGER
})
Relationships
LINKS_TO
Represents internal links between Wikipedia articles:

Cypher

CREATE (a1:Article)-[:LINKS_TO {
  link_type: STRING,     // Type of link (internal, redirect, etc.)
  anchor_text: STRING    // Link anchor text
}]->(a2:Article)
BELONGS_TO
Connects articles to their categories:

Cypher

CREATE (a:Article)-[:BELONGS_TO {
  sort_key: STRING       // Category sort key
}]->(c:Category)
REDIRECTS_TO
Handles Wikipedia redirects:

Cypher

CREATE (a1:Article)-[:REDIRECTS_TO]->(a2:Article)
Constraints and Indexes
Cypher

-- Unique constraints
CREATE CONSTRAINT article_id_unique FOR (a:Article) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT category_id_unique FOR (c:Category) REQUIRE c.id IS UNIQUE;

-- Performance indexes
CREATE INDEX article_title_index FOR (a:Article) ON (a.title);
CREATE INDEX article_namespace_index FOR (a:Article) ON (a.namespace);
CREATE INDEX category_name_index FOR (c:Category) ON (c.name);
Installation
Prerequisites
Python 3.8+

Neo4j Desktop or Neo4j Server 4.4+

Minimum 16GB RAM (32GB recommended for full dataset)

500GB+ available storage

Setup
Clone the repository

Bash

git clone https://github.com/ryderpongracic1/wikipedia-analysis.git
cd wikipedia-analysis
Install dependencies

Bash

pip install -r requirements.txt
Configure Neo4j connection

Bash

cp config/neo4j_config.example.py config/neo4j_config.py
# Edit config/neo4j_config.py with your Neo4j credentials
Download Wikipedia data

Bash

python scripts/download_data.py --date=latest
Import data into Neo4j

Bash

python scripts/import_data.py --batch-size=10000
Usage
Basic Analysis Queries
Find the most linked-to articles:

Cypher

MATCH (a:Article)<-[:LINKS_TO]-(b:Article)
RETURN a.title, count(b) as incoming_links
ORDER BY incoming_links DESC
LIMIT 10;
Find shortest path between two articles:

Cypher

MATCH (start:Article {title: "Albert Einstein"}),
      (end:Article {title: "Quantum Mechanics"}),
      path = shortestPath((start)-[:LINKS_TO*..6]-(end))
RETURN path, length(path) as degrees_of_separation;
Identify article communities:

Cypher

CALL gds.louvain.stream('citation-network')
YIELD nodeId, communityId
MATCH (a:Article) WHERE id(a) = nodeId
RETURN communityId, collect(a.title) as articles
ORDER BY communityId;
Running Analysis Scripts
Bash

# Calculate PageRank scores
python analysis/pagerank_analysis.py

# Find article communities
python analysis/community_detection.py

# Generate network statistics
python analysis/network_stats.py

# Export results
python analysis/export_results.py --format=csv
Project Structure
Plaintext

wikipedia-analysis/
├── config/                 # Configuration files
├── data/                   # Raw and processed data
├── scripts/                # Data processing scripts
├── analysis/               # Analysis modules
├── notebooks/              # Jupyter notebooks for exploration
├── results/                # Analysis outputs
├── tests/                  # Unit tests
├── requirements.txt        # Python dependencies
└── README.md               # This file
Analysis Examples
Six Degrees of Wikipedia
Find the shortest connection path between any two articles and analyze the "small world" properties of Wikipedia's knowledge graph.

Knowledge Authority Ranking
Implement PageRank and other centrality measures to identify the most authoritative articles in different domains.

Topic Clustering
Use community detection algorithms to discover how Wikipedia organizes knowledge into coherent topic clusters.

Citation Flow Analysis
Track how information and citations flow through the network to understand knowledge propagation patterns.

Performance Considerations
Memory Usage: Neo4j requires significant RAM for optimal performance with large graphs.

Batch Processing: Data import uses batching to manage memory usage.

Query Optimization: Complex traversals are optimized using Neo4j's graph algorithms library.

Indexing Strategy: Strategic indexing on frequently queried properties improves performance.

Contributing
Fork the repository.

Create a feature branch (git checkout -b feature/new-analysis).

Commit your changes (git commit -am 'Add new analysis').

Push to the branch (git push origin feature/new-analysis).

Create a Pull Request.

Future Enhancements
Multi-language Wikipedia support

Real-time data updates

Web-based visualization dashboard

Machine learning integration for link prediction

Temporal network analysis with historical dumps

License
This project is licensed under the MIT License - see the LICENSE file for details.

Acknowledgments
Wikipedia and the Wikimedia Foundation for providing open access to their data.

Neo4j for their excellent graph database platform.

The research community working on network analysis and knowledge graphs.

Contact
Ryder Pongracic - GitHub

Project Link: https://github.com/ryderpongracic1/wikipedia-analysis