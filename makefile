duck:
	poetry run python py/extract_load.py
	npm --prefix ./duckdb_e2e/reports/ run dev
