from setuptools import find_packages, setup

setup(
    name="openlibrary_dagster",
    packages=find_packages(exclude=["openlibrary_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-embedded-elt"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
