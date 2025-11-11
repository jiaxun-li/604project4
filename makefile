##### Config #####
ROOT          := $(CURDIR)
PYTHON        ?= python3

VENV_DIR      ?= $(ROOT)/.venv
PIP           ?= $(VENV_DIR)/bin/pip
RUN           ?= $(VENV_DIR)/bin/python

APP_DIR       ?= src


DATE          ?= 2025-11-12
ZONES-JSON    ?= config/zone_coords.json
MODEL         ?= models/baseline_hist_avg.parquet

IMAGE         ?= 604project4:latest
CONTAINER     ?= 604project4

##### Helpers #####
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' Makefile | sed 's/:.*##/: /' | sort

##### Local dev #####
.PHONY: venv
venv: ## Create virtualenv
	$(PYTHON) -m venv $(VENV_DIR)

.PHONY: install
install: venv ## Install Python deps
	$(PIP) install --upgrade pip
	@if [ -f requirements.txt ]; then $(PIP) install -r requirements.txt; fi

.PHONY: format
format: ## Format with black (if installed)
	-$(VENV_DIR)/bin/black $(APP_DIR)

.PHONY: lint
lint: ## Lint with ruff (if installed)
	-$(VENV_DIR)/bin/ruff check $(APP_DIR)

##### Project commands (run from src/) #####
# NOTE: all commands cd into $(APP_DIR) so relative paths work.
.PHONY: run-predict
run-predict: ## Run next-day prediction; pass DATE=YYYY-MM-DD, ZONES=..., MODEL=...
	cd $(APP_DIR) && \
	$(RUN) baseline_model.py predict \
		--date $(DATE) \
		--model $(MODEL) 

.PHONY: update-weather
update-weather: ## Update only weather data
	cd $(APP_DIR) && \
	$(RUN) renew_weather_data.py \
	    --zones-json $(ZONES-JSON)

##### Data hygiene #####
.PHONY: clean
clean: ## Remove caches
	rm -rf __pycache__ .pytest_cache .ruff_cache $(APP_DIR)/__pycache__ $(APP_DIR)/**/__pycache__

.PHONY: clean-data
clean-data: ## Remove large outputs (customize)
	rm -rf $(APP_DIR)/predictions/*

##### Docker #####
.PHONY: docker-build
docker-build: ## Build Docker image
	docker build -t $(IMAGE) .

.PHONY: docker-run-bash
docker-run-bash: ## Interactive shell in container (cwd=/app/src)
	docker run --rm -it --name $(CONTAINER) \
		-v $$(pwd):/app \
		-w /app/$(APP_DIR) \
		--env TZ=America/Detroit \
		$(IMAGE) bash

.PHONY: docker-predict
docker-predict: ## Run prediction in Docker
	docker run --rm -it --name $(CONTAINER) \
		-v $$(pwd):/app \
		-w /app/$(APP_DIR) \
		--env TZ=America/Detroit \
		$(IMAGE) \
		python baseline_model.py predict \
			--date $(DATE) \
			--model $(MODEL)

.PHONY: docker-update-weather
docker-update-weather: ## Update weather in Docker
	docker run --rm -it --name $(CONTAINER) \
		-v $$(pwd):/app \
		-w /app/$(APP_DIR) \
		--env TZ=America/Detroit \
		$(IMAGE) \
		python renew_weather_data.py \
		--zones-json $(ZONES-JSON)
