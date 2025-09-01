# Wikipedia Citation Network Analysis

## Database System Selection

After careful consideration, Neo4j has been selected as the database system for this project.

### Reasons for Choosing Neo4j

*   **Graph Database:** Neo4j is a dedicated graph database, which is well-suited for analyzing relationships between data points, such as Wikipedia articles and their citations.
*   **Scalability:** Neo4j is highly scalable and can handle large datasets, such as the Wikipedia citation network (10 million articles and 100 million links).
*   **Query Performance:** Neo4j supports efficient querying of graph data, which is essential for finding shortest paths and identifying influential nodes.
*   **Mature Ecosystem:** Neo4j has a mature ecosystem and a large community, which provides ample resources and support for development.

### Strengths

*   Excellent performance for graph-based queries, such as finding shortest paths and identifying influential nodes.
*   Easy to use and learn, with a user-friendly query language (Cypher).
*   Large community and ample resources for development.

### Weaknesses

*   May not be the best choice for non-graph data.
*   Can be more expensive than other database systems.

### Alternatives Considered

*   **Amazon Neptune:** A fully managed graph database service that offers high scalability and availability. However, Neo4j's mature ecosystem and large community made it a slightly better choice for this project.
*   **PostgreSQL with graph extensions:** A relational database with graph database extensions. This option could be suitable if the project already used PostgreSQL or if there was a need to combine graph data with relational data. However, for a dedicated graph analysis project, Neo4j is the preferred option.
## Database Schema

This section describes the Neo4j database schema used for the Wikipedia citation network analysis.

### Nodes

*   **Article:** Represents a Wikipedia article.
    *   Properties: `id`, `title`, `url`

### Relationships

*   **CITES:** Represents a citation between two articles.
    *   Properties: None

### Constraints

*   `Article.id` is a unique constraint.

### Indexes

*   Index on `Article.title` for faster lookups.

**Note:** This is a placeholder schema. Please replace with the actual schema details.