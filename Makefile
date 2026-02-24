PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
UVICORN := $(VENV)/bin/uvicorn
ALEMBIC := $(VENV)/bin/alembic

.PHONY: venv install test run migrate migrate-up migrate-down migrate-revision

venv:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -r backend/requirements.txt

test:
	cd backend && ../$(PYTEST) -q

run:
	cd backend && ../$(UVICORN) app.main:app --reload

migrate-up:
	cd backend && ../$(ALEMBIC) -c alembic.ini upgrade head

migrate-down:
	cd backend && ../$(ALEMBIC) -c alembic.ini downgrade -1

migrate-revision:
	cd backend && ../$(ALEMBIC) -c alembic.ini revision -m "$(m)"


migrate:
	cd backend && ../$(ALEMBIC) -c alembic.ini upgrade head
