from setuptools import find_packages, setup

setup(
    name="olist_dagster",
    packages=find_packages(exclude=["olist_dagster_tests"]),
    install_requires=[
        "dagster",
        "python-dotenv",
        "google-cloud-bigquery",
        "db-dtypes",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
