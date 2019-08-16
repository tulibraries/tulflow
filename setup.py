import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tulflow",
    version="0.0.1",
    author="Temple University Libraries",
    author_email="tul08567@temple.edu",
    description="Package of Temple University Library Indexing & ETL functions used by Airflow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tulibraries/tulflow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
)
