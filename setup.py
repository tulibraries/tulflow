"""tulflow Python package setup."""

import os
import sys
import setuptools
from setuptools.command.install import install

VERSION = "v0.9.9"

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)

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
    test_suite="nose.collector",
    tests_require=["nose"],
    setup_requires=["setuptools>=17.1"],
	version=VERSION,
	cmdclass={
        "verify": VerifyVersionCommand,
    }
)
