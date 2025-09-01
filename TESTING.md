# Testing Documentation: Wikipedia Citation Network Analysis

This document provides a comprehensive overview of the testing strategy and implementation for the Wikipedia Citation Network Analysis project. Our goal is to ensure the reliability, correctness, and performance of the data processing, database interactions, and analytical components.

## Testing Frameworks

We utilize `pytest` as our primary testing framework, complemented by several plugins to enhance our testing capabilities:

*   **`pytest`**: The core framework for writing and running tests. It provides a simple yet powerful way to define test functions and classes.
*   **`pytest-mock`**: A plugin that simplifies mocking and patching in tests, allowing us to isolate units of code and control their dependencies.
*   **`pytest-cov`**: Used for measuring code coverage, ensuring that a high percentage of our codebase is exercised by tests.

## Test Organization

All test files are located within the `tests/` directory. This directory is structured to mirror the main application's module structure, making it easy to locate tests for specific components.

## Shared Fixtures (`tests/conftest.py`)

The `tests/conftest.py` file contains shared fixtures that provide reusable setup and teardown logic for various tests. Key fixtures include:

*   **`mock_neo4j_session`**: Mocks the Neo4j database session, allowing unit tests to run without an actual database connection.
*   **`sample_article_data`**: Provides sample Wikipedia article data for testing data processing and import functionalities.
*   **`sample_category_data`**: Provides sample Wikipedia category data for testing data processing and import functionalities.
*   **`mock_config`**: Mocks the application's configuration settings, enabling tests to run with controlled parameters.
*   **`neo4j_container`**: Manages a Dockerized Neo4j instance using `testcontainers-python` for integration tests.
*   **`neo4j_driver`**: Provides a Neo4j driver connected to the `neo4j_container` for integration tests.
*   **`populated_neo4j_db`**: A fixture that sets up and populates a Neo4j database with sample data for integration tests, ensuring a consistent state.

## Unit Tests

Unit tests focus on individual components or functions in isolation.

*   **`tests/test_config.py`**:
    *   **Purpose**: Verifies the correct loading, validation, and formatting of application configuration.
    *   **Scenarios Covered**: Configuration loading from various sources (e.g., environment variables), validation of required settings, correct formatting of Neo4j connection strings, and mock connection attempts.
*   **`tests/test_data_processing.py`**:
    *   **Purpose**: Ensures the robust parsing, cleaning, and transformation of Wikipedia dump data.
    *   **Scenarios Covered**: Wikipedia dump parsing, data cleaning/validation, batch processing of data, error handling for corrupted or malformed data, and data transformation into a format suitable for Neo4j.
*   **`tests/test_database.py`**:
    *   **Purpose**: Validates interactions with the Neo4j database, including connection management, data insertion, and schema creation.
    *   **Scenarios Covered**: Neo4j connection establishment and termination, node and relationship creation, constraint and index creation, efficient batch imports, transaction handling, and data integrity checks.
*   **`tests/test_analysis.py`**:
    *   **Purpose**: Tests the correctness of graph analysis algorithms and result handling.
    *   **Scenarios Covered**: Calculation of graph metrics like PageRank, shortest path, community detection, and centrality measures. It also covers result formatting/export and tests with mock query results.
*   **`tests/test_queries.py`**:
    *   **Purpose**: Verifies the construction, sanitization, and optimization of Cypher queries.
    *   **Scenarios Covered**: Correct Cypher query construction for various operations, parameter sanitization to prevent injection attacks, query optimization techniques, variations of queries, and syntax validation.

## Integration Tests (`tests/test_integration.py`)

Integration tests verify the interaction between multiple components, often involving a real Neo4j database.

*   **Purpose**: To ensure that different modules of the application work together seamlessly and that the entire system behaves as expected.
*   **`testcontainers-python`**: Used to spin up a temporary, isolated Neo4j database instance for each integration test run, ensuring a clean and consistent testing environment.
*   **End-to-End Scenarios Covered**:
    *   **Data Import**: Verifies the entire data ingestion pipeline, from raw Wikipedia data to populated Neo4j database.
    *   **Analysis Pipeline**: Tests the execution of graph analysis algorithms on real data within the Neo4j database.
    *   **Data Export**: Confirms that analysis results can be correctly extracted and formatted.
    *   **Data Integrity**: Checks that data remains consistent and accurate throughout the processing and analysis stages.
    *   **Performance Benchmarking**: (Where applicable) Measures the performance of critical operations.

## Running Tests

To run the tests locally:

1.  **Install dependencies**: Ensure all project dependencies, including `pytest` and its plugins, are installed.
2.  **Execute `pytest`**: Navigate to the root directory of the project and run:
    ```bash
    pytest
    ```
    This will execute all unit and integration tests.

To generate a code coverage report:

```bash
pytest --cov=wikipedia_analysis --cov-report=term-missing --cov-report=html
```
This command will run the tests, display a coverage report in the terminal, and generate an HTML report in the `htmlcov/` directory.

## Code Coverage

We aim for a target code coverage of **90%+** to ensure a high level of test confidence. Code coverage is tracked using `pytest-cov` and is a key metric in our development process.

## CI/CD Integration

Automated testing is integrated into our Continuous Integration/Continuous Deployment (CI/CD) pipeline via GitHub Actions. The workflow defined in `.github/workflows/ci.yml` automatically runs all tests and checks code coverage on every push and pull request, ensuring that new changes do not introduce regressions and maintain code quality standards.