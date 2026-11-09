# Makefile — DE Agentic Boilerplate HTAP (Template Mestre Multi-Model)
SHELL := /bin/bash
UV := /home/Hugo/.local/bin/uv
PYTHON := $(UV) run python

# Resolve o binário do datacontract com fallback inteligente
DATACONTRACT := $(shell if [ -f "./.venv/bin/datacontract" ]; then echo "./.venv/bin/datacontract"; \
	elif [ -f "/home/Hugo/comex-data-engine-v2/.venv/bin/datacontract" ]; then echo "/home/Hugo/comex-data-engine-v2/.venv/bin/datacontract"; \
	elif [ -f "/home/Hugo/de-agentic-boilerplate-olap/.venv/bin/datacontract" ]; then echo "/home/Hugo/de-agentic-boilerplate-olap/.venv/bin/datacontract"; \
	elif command -v datacontract >/dev/null 2>&1; then echo "datacontract"; \
	else echo "$(UV) run datacontract"; fi)

.PHONY: help setup up down ps logs test test-unit test-integration contract-lint run-api verify clean all

help:
	@echo "======================================================================"
	@echo " DE Agentic Boilerplate HTAP — Multi-Model Template Mestre"
	@echo "======================================================================"
	@echo "  make setup            -> Sincroniza ambiente virtual (.venv via uv)"
	@echo "  make up               -> Inicia PostgreSQL 16 & MongoDB 7 (docker compose)"
	@echo "  make down             -> Para os containers"
	@echo "  make ps               -> Verifica saúde dos bancos (healthy status)"
	@echo "  make logs             -> Acompanha logs dos containers"
	@echo "  make test             -> Roda todos os testes automatizados (unit + integration)"
	@echo "  make test-unit        -> Roda apenas testes unitários rápidos"
	@echo "  make test-integration -> Roda testes de integração contra MongoDB, Postgres e DuckDB"
	@echo "  make contract-lint    -> Valida contratos de dados ODCS v3.2.0"
	@echo "  make verify           -> Executa o Checklist de Certificação HTAP (5 Pontos)"
	@echo "  make run-api          -> Inicia a API FastAPI (http://localhost:8000)"
	@echo "  make clean            -> Limpa arquivos temporários e caches"

setup:
	$(UV) sync

up:
	docker compose up -d
	@echo "Containers iniciados. Use 'make ps' para verificar a saúde dos bancos."

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	$(UV) run pytest tests/ -v

test-unit:
	$(UV) run pytest tests/ -v -m unit

test-integration:
	$(UV) run pytest tests/ -v -m integration

contract-lint:
	@for f in ./contracts/*.odcs.yaml; do \
		echo "🔍 Linting $$f..."; \
		$(DATACONTRACT) lint "$$f" || exit 1; \
	done

run-api:
	$(UV) run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache

verify: contract-lint test
	@echo ""
	@echo "======================================================================"
	@echo " 🎖️  SELO DE CERTIFICAÇÃO HTAP — 5 PONTOS DE CONFORMIDADE"
	@echo "======================================================================"
	@echo " [1/5] Contratos de Dados ODCS v3.2.0: APROVADO"
	@echo " [2/5] Modelos Fiduciários Pydantic V2 (No-Float): APROVADO"
	@echo " [3/5] API FastAPI de Ingestão de Borda: APROVADA"
	@echo " [4/5] Conectividade Multi-Model (Mongo 27017, Postgres 5433, DuckDB): VALIDADA"
	@echo " [5/5] Idempotência e Particionamento Hive Snappy: TESTADOS"
	@echo "======================================================================"
	@echo " 🚀 Template HTAP pronto para especialização em qualquer projeto!"
	@echo "======================================================================"

all: up contract-lint test
