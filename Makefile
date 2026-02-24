lint:
	pipenv run pylint tulflow -E

test:
	PYTHONPATH=. pipenv run pytest

build-requirements:
	.github/shared-scripts/build-requirements.sh

rebuild-pipfile: build-requirements
	pipenv --rm
	rm -f Pipfile.lock
	pipenv install --dev --requirements pipfile-requirements.txt

compare-dependencies:
	.github/shared-scripts/compare_dependencies.sh
