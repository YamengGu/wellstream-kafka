.PHONY: data install test lint up replay down backfill dictionary

data:            ## download the Volve logs (checksummed)
	bash scripts/fetch_data.sh data

install:
	pip install -e '.[dev]'

lint:
	ruff check .

test:            ## unit tests; set WELLSTREAM_TEST_PG_DSN / WELLSTREAM_TEST_KAFKA for the integration tests
	pytest -q

up: data         ## start Kafka, Schema Registry, TimescaleDB, Grafana and the services
	docker compose up -d --build

replay:          ## stream F-1 C into Kafka at 50x real time
	docker compose run --rm replay --well F-1C --speedup 50

down:
	docker compose down -v

backfill: data   ## batch path: both wells straight into the warehouse, with backtest reports
	wellstream backfill --well F-1C --load --report reports/backtest_F-1C.json > /dev/null
	wellstream backfill --well F-5 --load --report reports/backtest_F-5.json > /dev/null
	wellstream refresh-marts

dictionary:      ## regenerate docs/data_dictionary.md from the live warehouse
	wellstream data-dictionary > docs/data_dictionary.md
