from setuptools import find_packages, setup

setup(
    name="data_pipeline",
    version="1.0.0",
    description="ETL job.",
    packages=find_packages(exclude=("test*",)),
    python_requires=">=3.8,<3.9",
    install_requires=[
        "pyspark",
        "delta-spark",
    ],
    long_description_content_type="text/markdown",
)
