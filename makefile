##### Config #####
.DEFAULT_GOAL := all
ROOT          := $(CURDIR)
PYTHON        ?= python3

VENV_DIR      ?= $(ROOT)/.venv
PIP           ?= $(VENV_DIR)/bin/pip
RUN           ?= $(PYTHON)

APP_DIR       ?= src


DATE          ?= $(shell $(PYTHON) -c 'from datetime import datetime, timedelta; from zoneinfo import ZoneInfo; tz = ZoneInfo("America/New_York"); tomorrow = datetime.now(tz).date() + timedelta(days=1); floor = datetime(2025, 11, 20).date(); print(max(floor, tomorrow).isoformat())')
ZONES-JSON    ?= config/zone_coords.json
MODEL         ?= models/baseline_hist_avg.parquet

IMAGE         ?= limitlessxun/604project4:latest
API_KEY       ?= dde83048f98f49a3b7563a2421e9b8dd
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

ARTIFACT_DIRS := \
	$(APP_DIR)/predictions \
	$(APP_DIR)/plots \
	$(APP_DIR)/tmpplots \
	$(APP_DIR)/data/processed

.PHONY: predictions
predictions: ## Print + log contest predictions for DATE=YYYY-MM-DD (default: script auto-selects)
	@cd $(APP_DIR) && \
		export PJM_API_KEY=$(API_KEY) && \
		$(RUN) renew_data.py --zones-json $(ZONES-JSON) >/dev/null 2>&1 && \
		$(RUN) export_training_data.py >/dev/null 2>&1 && \
		$(RUN) make_predictions.py --date $(DATE) 2>/dev/null | tail -n 1

.PHONY: rawdata
rawdata: ## Refresh raw PJM + weather data from scratch
	@if [ -z "$$PJM_API_KEY" ]; then \
		echo "Warning: PJM_API_KEY not set; using default API key from Makefile"; \
	fi
	@rm -rf $(APP_DIR)/data/raw/*
	@cd $(APP_DIR) && \
		export PJM_API_KEY=$(API_KEY) && \
		$(RUN) pjm_download.py --source osf && \
		$(RUN) weather_download.py download --zones-json $(ZONES-JSON) --out data/raw/noaa_hourly.csv && \
		$(RUN) weather_download.py split --in data/raw/noaa_hourly.csv --outdir data/raw/weather --overwrite && \
		$(RUN) renew_data.py --zones-json $(ZONES-JSON)

.PHONY: renew
renew: ## Renew a single month's PJM + weather CSVs (requires PJM_API_KEY)
	@export PJM_API_KEY=$(API_KEY); \
	cd $(APP_DIR) && \
		$(RUN) renew_data.py --zones-json $(ZONES-JSON)

.PHONY: export-training
export-training: ## Export training PJM + weather CSVs into data/processed
	@cd $(APP_DIR) && \
		$(RUN) export_training_data.py

.PHONY: export-testing
export-testing: ## Export testing PJM + weather CSVs into data/processed
	@cd $(APP_DIR) && \
		$(RUN) export_test_data.py

.PHONY: all
all: ## Run renew + exports and retrain all models
	@export PJM_API_KEY=$(API_KEY); \
	$(MAKE) renew; \
	$(MAKE) export-training; \
	$(MAKE) export-testing; \
	cd $(APP_DIR) && \
		$(RUN) xgboost_hourly_load.py --force-retrain && \
		$(RUN) xgboost_daily_peak.py --force-retrain&& \
		$(RUN) xgboost_peak_hour.py --force-retrain


.PHONY: clean
clean: ## Remove caches
	@find $(ROOT) -type d -name '__pycache__' -prune -exec rm -rf {} +
	@find $(ROOT) -name '.DS_Store' -delete
	@rm -rf .pytest_cache .ruff_cache $(ARTIFACT_DIRS) $(VENV_DIR) $(APP_DIR)/model

.PHONY: clean-predictions
clean-predictions: ## Remove generated prediction files
	@rm -rf $(APP_DIR)/predictions/*


##### Docker #####

.PHONY: docker-build
docker-build: ## Build Docker image
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-t $(IMAGE) \
		--push .
