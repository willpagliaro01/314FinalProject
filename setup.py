from setuptools import find_packages, setup

setup(
    name="314_final",
    packages=find_packages(exclude=["314_final_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pytest",
        "kaggle",
        "dagster",
        "pandas",
        "os",
        "zipfile",
        "csv",
        "unittest"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
