from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="wikipedia-analysis",
    version="1.0.0",
    author="Ryder Pongracic",
    author_email="ryderjpm@gmail.com",
    description="Wikipedia to Neo4j Importer & Citation Network Analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ryderpongracic1/wikipedia-analysis",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "neo4j>=5.0",
        "python-dotenv>=1.0",
        "lxml>=4.6",
        "flask>=2.0"
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-mock>=3.0",
            "pytest-cov>=4.0",
            "testcontainers-neo4j>=0.0.1rc1",
        ]
    },
    entry_points={
        "console_scripts": [
            "wikipedia-import=wikipedia_analysis.import_with_links:main",
            "wikipedia-analyze=wikipedia_analysis.run_analysis:main",
            "wikipedia-api=wikipedia_analysis.api:main",
        ]
    },
)
