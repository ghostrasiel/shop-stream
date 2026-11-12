.PHONY: up down logs ps check reset seed test

# Detached. `docker compose up --build` (no -d) also works if you want the logs in the foreground.
up: .env
	docker compose up -d --build --remove-orphans

.env:
	cp .env.example .env

down:
	docker compose down

# Stops the stack and deletes Kafka, Postgres, Mongo, and Spark checkpoint data.
reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

check:
	./scripts/check.sh

# Re-run the catalog load after editing catalog/products.json.
seed:
	docker compose run --rm --build catalog-seed

test:
	python3 -m unittest discover -s tests -v
