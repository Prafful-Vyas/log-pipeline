.PHONY: up down logs restart psql produce test lint build bench load chaos

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f indexer producer alerter

restart:
	docker compose restart indexer producer alerter

psql:
	docker compose exec postgres psql -U logs -d logs

produce:
	docker compose run --rm producer python -m services.producer --rate 2000 --duration 60

build:
	docker compose build

test:
	pytest tests/unit -v

lint:
	ruff check common services tests bench scripts

RATES ?= 1000,2000,5000
STEP ?= 30

bench:
	python -m bench.bench_parser
	docker compose run --rm indexer python -m bench.bench_write_strategies

load:
	docker compose run --rm producer python -m bench.load_test --rates $(RATES) --step-duration $(STEP)

chaos:
	pytest tests/load -v -s
