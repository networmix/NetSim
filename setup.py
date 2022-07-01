from setuptools import setup, find_packages


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ngraph",
    version="0.2.0",
    author="Andrey Golovanov",
    description="A discrete event simulator toolkit adapted for network simulation use-cases.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/networmix/netsim",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=("tests", "dev")),
    python_requires=">=3.8",
    tests_require=["pytest"],
)
