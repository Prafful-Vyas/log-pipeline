.PHONY: up down logs restart psql produce test lint build

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
	ruff check common services tests
