lint:
	poetry run flake8 src/ tests/

run-main:
	poetry run python src/main.py

setup:
	poetry install
	poetry run pre-commit install

test:
	poetry run pytest
