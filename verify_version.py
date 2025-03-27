import os
import sys

VERSION = "v0.12.6"  # Ensure this matches the version in pyproject.toml

def verify_version():
    """Verify that the git tag matches the project version."""
    tag = os.getenv('CIRCLE_TAG')

    if tag != VERSION:
        info = f"Git tag: {tag} does not match the version of this app: {VERSION}"
        sys.exit(info)

if __name__ == "__main__":
    verify_version()