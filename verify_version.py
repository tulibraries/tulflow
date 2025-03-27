import os
import sys

def get_version_from_file():
    try:
        with open("VERSION", "r") as version_file:
            return version_file.read().strip()
    except FileNotFoundError:
        sys.exit("VERSION file not found. Please ensure it exists in the project root.")

VERSION = get_version_from_file()  # Dynamically read the version from the VERSION file

def verify_version():
    """Verify that the git tag matches the project version."""
    tag = os.getenv('CIRCLE_TAG')

    if tag != VERSION:
        info = f"Git tag: {tag} does not match the version of this app: {VERSION}"
        sys.exit(info)

if __name__ == "__main__":
    verify_version()