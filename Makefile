PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
UVICORN := $(VENV)/bin/uvicorn
ALEMBIC := $(VENV)/bin/alembic

ifeq ("$(wildcard .venv/bin/python)","")
	PY=python3
else
	PY=.venv/bin/python
endif

.PHONY: venv install test run run-scheduler migrate migrate-up migrate-down migrate-revision

venv:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -r backend/requirements.txt

test:
	cd backend && $(PY) -m pytest -q

run:
	cd backend && $(PY) -m uvicorn app.main:app --reload

run-scheduler:
	cd backend && ENABLE_SCHEDULER=true $(PY) -m uvicorn app.main:app --reload

migrate-up:
	cd backend && ../$(ALEMBIC) -c alembic.ini upgrade head

migrate-down:
	cd backend && ../$(ALEMBIC) -c alembic.ini downgrade -1

migrate-revision:
	cd backend && ../$(ALEMBIC) -c alembic.ini revision -m "$(m)"


migrate:
	cd backend && ../$(ALEMBIC) -c alembic.ini upgrade head
