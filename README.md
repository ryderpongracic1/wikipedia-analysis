Both `Article` and `Category` carry a unique `id` property backed by a database constraint. Articles also store `title`, `namespace`, `length`, `is_redirect`, and `is_minor`.

## Testing

```bash
# Unit tests — no Neo4j needed (~98 tests, < 1 s)
python -m pytest tests/ -m "not integration and not benchmark"

# Integration tests — requires a running Neo4j instance
NEO4J_URI=bolt://localhost:7687 \
NEO4J_USER=neo4j \
NEO4J_PASSWORD=your_password \
  python -m pytest tests/ -m "integration and not benchmark" -v

# Pure-Python benchmarks — no Neo4j needed
python -m pytest tests/test_benchmarks.py -m "benchmark and not integration"

# All benchmarks including GDS algorithms — requires Neo4j + GDS
NEO4J_URI=bolt://localhost:7687 \
NEO4J_USER=neo4j \
NEO4J_PASSWORD=your_password \
  python -m pytest tests/test_benchmarks.py -m benchmark -v
```

### Pytest markers

| Marker | Meaning |
|--------|---------|
| `integration` | Requires a live Neo4j instance |
| `benchmark` | Records timing; writes JSON to `benchmarks/results/` |
| `slow` | Tests expected to take > 5 s |

## Performance Benchmarks

All benchmarks use [`measure_performance()`](wikipedia_analysis/analysis.py) (`time.perf_counter`, N = 10 repeated runs, min/mean/p95 reported). Results are written to `benchmarks/results/<ISO_timestamp>.json` and uploaded as 90-day artifacts by the nightly CI workflow.

---

### Data-processing pipeline

*Measured on Python 3.12.2, Apple M-series CPU. No Neo4j connection required.*

| Operation | Mean | p95 | Throughput |
|-----------|-----:|----:|----------:|
| `parse_dump_file` — 4-article XML | 0.24 ms | 0.80 ms | — |
| `transform_to_article_node` — 1 000 items | 0.32 ms | 0.37 ms | ~3.1 M nodes/s |
| `batch_data` — batch size 10 | 0.015 ms | 0.019 ms | — |
| `batch_data` — batch size 100 | 0.013 ms | 0.014 ms | — |
| `batch_data` — batch size 500 | 0.014 ms | 0.015 ms | — |

---

### Cypher query-builder latency

*Pure Python, no network I/O. These are the cost of constructing the parameterised query strings before they are sent to Neo4j.*

| Builder | Mean | p95 |
|---------|-----:|----:|
| `build_pagerank_query` | < 0.001 ms | 0.005 ms |
| `build_shortest_path_query` | < 0.001 ms | 0.001 ms |
| `build_community_detection_query` | < 0.001 ms | 0.001 ms |
| `build_batch_create_articles_query` — 10 nodes | 0.002 ms | 0.004 ms |
| `build_batch_create_articles_query` — 100 nodes | 0.013 ms | 0.014 ms |
| `build_batch_create_articles_query` — 1 000 nodes | 0.128 ms | 0.130 ms |

Builder cost is CPU-bound and scales linearly with batch size (~0.13 µs per node). It is not on the critical path for production imports.

---

### Neo4j query latency and GDS algorithm convergence

*Requires a running Neo4j 4.4 instance with the GDS plugin. Benchmarks run nightly in CI against the integration-test fixture graph (4 articles, 2 categories). Production times on the full English Wikipedia (~7 M articles, ~120 M links) will be orders of magnitude higher and depend heavily on hardware and JVM heap configuration.*

| Operation | Typical range |
|-----------|:-------------:|
| GDS named-graph projection | 5 – 50 ms |
| **PageRank** — convergence (20 iterations, damping 0.85) | 10 – 100 ms |
| **Shortest path** — Dijkstra / BFS single pair | 1 – 20 ms |
| **Louvain community detection** | 10 – 150 ms |
| **Betweenness centrality** | 20 – 200 ms |
| **Closeness centrality** | 10 – 100 ms |
| Batch node import — 100 nodes (`MERGE + SET`) | 5 – 30 ms |
| Batch node import — 1 000 nodes | 20 – 150 ms |
| Batch relationship import — 100 rels | 5 – 30 ms |
| `GET /categories` API latency (round-trip) | 2 – 15 ms |
| `GET /category/<name>` API latency (round-trip) | 2 – 15 ms |

GDS algorithm benchmarks are automatically skipped when the GDS library is not installed (e.g. plain `neo4j:4.4` Docker image used in unit-test CI).

To record your own GDS benchmark baseline:

```bash
NEO4J_URI=bolt://localhost:7687 \
NEO4J_USER=neo4j \
NEO4J_PASSWORD=your_password \
  pytest tests/test_benchmarks.py -m benchmark -v
# → benchmarks/results/20260429T....json
```

---

## CI

| Workflow | Trigger | Matrix | Purpose |
|----------|---------|--------|---------|
| `Python CI` | Push / PR to `main` | Python 3.9, 3.10, 3.11 | Lint, unit tests, integration tests |
| `Benchmarks` | Nightly 06:00 UTC + manual dispatch | Python 3.11 | Full benchmark suite; JSON artifacts retained 90 days |

Integration tests connect to a Neo4j 4.4 service container provisioned by GitHub Actions using the env vars `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASSWORD`. The `neo4j_driver` fixture detects these and skips spawning a second container via testcontainers.

## Database Migration

The canonical article→category relationship is `BELONGS_TO`. If you imported data with an older version of this project that used `IN_CATEGORY`, migrate with:

```cypher
MATCH (a)-[r:IN_CATEGORY]->(c)
MERGE (a)-[:BELONGS_TO]->(c)
DELETE r
```
