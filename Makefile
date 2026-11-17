.PHONY: up down logs ps check reset seed test dbt dashboards

# Detached. `docker compose up --build` (no -d) also works if you want the logs in the foreground.
up: .env
	docker compose up -d --build --remove-orphans

.env:
	cp .env.example .env

down:
	docker compose down

# Stops the stack and deletes every named volume in this compose file
# (Kafka, Postgres, Mongo, Spark checkpoints, Prometheus).
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

dbt:
	docker compose --profile dbt run --rm --build dbt

dashboards:
	@echo "Grafana     http://localhost:$${GRAFANA_PORT:-3000}  admin / admin"
	@echo "Prometheus  http://localhost:$${PROMETHEUS_PORT:-9090}/alerts"
	@echo "Spark UI    http://localhost:$${SPARK_UI_PORT:-4040}"
