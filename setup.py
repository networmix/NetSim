from setuptools import setup, find_packages


with open("README.md", encoding="utf8") as f:
    readme = f.read()

with open("LICENSE", encoding="utf8") as f:
    lic = f.read()

setup(
    name="NetSim",
    version="0.1.0",
    description="Toolkit helping (eventually) to reason about network designs",
    long_description=readme,
    license=lic,
    packages=find_packages(exclude=("tests", "docs")),
)
