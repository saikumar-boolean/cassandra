from setuptools import find_packages, setup

setup(
    name="controles_laurentide_data_lake",
    packages=find_packages(exclude=["controles_laurentide_data_lake_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
