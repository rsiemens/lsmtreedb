format:
	poetry run black lsmtree
	poetry run isort lsmtree
	poetry run black test
	poetry run isort test

test:
	poetry run pytest 

.PHONY: format test
