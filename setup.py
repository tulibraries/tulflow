"""tulflow Python package setup."""
import setuptools

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

setuptools.setup(
    name="tulflow",
    author="Temple University Libraries",
    author_email="tul08567@temple.edu",
    description="Package of Temple University Library Indexing & ETL functions used by Airflow.",
    long_description=LONG_DESCRIPTION,
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
    setup_requires=['setuptools>=17.1'],
    version="0.6.1"
)
