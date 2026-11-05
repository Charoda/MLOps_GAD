.DEFAULT_GOAL := help
COMPOSE := docker compose

help:
	@echo "up | down | clean | test | unit | check | health | cli C='dags list' | sql"

up:
	$(COMPOSE) up -d --build --wait --wait-timeout 300

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v
	python3 tools/clean_artifacts.py

test:
	$(COMPOSE) exec -T scheduler python -m pytest /opt/airflow/tests -q

unit:
	$(COMPOSE) run --rm --no-deps scheduler python -m pytest /opt/airflow/tests/test_core.py -q

check:
	$(COMPOSE) exec -T scheduler python -m pytest /opt/airflow/tests/test_dags.py -q

health:
	$(COMPOSE) exec -T scheduler python /opt/airflow/include/health.py

cli:
	$(COMPOSE) exec -T scheduler airflow $(C)

sql:
	$(COMPOSE) exec warehouse psql -U etl -d warehouse

.PHONY: help up down clean test unit check health cli sql
