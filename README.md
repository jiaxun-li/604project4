# 604 Project 4: PJM Load Forecasting Toolkit

This project automates the workflow for producing predictions for three related PJM forecasting targets:

1. **Hourly zone load forecasts** predict the hourly load per zone/hour of a day using gradient-boosted regression (one XGBoost model per zone/hour).
2. **Peak-hour prediction** predict the peak hour (defined as the hour which has the maximum load) per zone/day via gradient boosting.
3. **10-day peak-day window prediction** predict whether a target date is the peak day out of a 10 days window (defined as the 2 days out of the 10 with the highest load during the peak hour for each zone) vias gradient-boosted regression.

All scripts live under `src/` and share cached model artifacts in `src/model/...`.
The default target day is the next day when the script run. The default 10 days window is from 2025-11-20 to 2025-11-29.

## 1. Downloading & Refreshing Data

The `Makefile` provides convenience targets that wrap the data pipeline. Before running anything, ensure Python 3.10+ is installed and (optionally) create a virtual environment via `make venv && make install`.

### Refresh The Processed Training/Test Splits

```bash
make all
```

`make all` performs:

- `make renew`: refreshes PJM + weather CSVs (requires `PJM_API_KEY`; default pulled from `Makefile`).
- `make export-training` / `make export-testing`: builds the processed datasets under `src/data/processed`.
- Retrains all cached models (`xgboost_hourly_load.py`, `xgboost_daily_peak.py`, `xgboost_peak_hour.py`) with `--force-retrain`.

### Rebuild Raw Data From Scratch

```bash
make rawdata
```

This wipes `src/data/raw`, downloads PJM load data, grabs NOAA hourly weather, splits it by zone, and runs `renew_data.py` to align the files.

### One-Off Updates

You can run each step individually:

- `make renew` – refresh monthly PJM/weather CSVs (relies on `src/renew_data.py`).
- `make export-training` / `make export-testing` – regenerate processed splits.
- `make clean` – remove caches, predictions, venv, and cached models.
- `make clean-predictions` – only delete files under `src/predictions/`.


## 2. The Three Prediction Tasks

All tasks assume the processed data exists under `src/data/processed/`. Model artifacts persist under `src/model/...` so re-runs skip training unless `--force-retrain` is provided.

### Task A — Hourly Load Forecasts

Script: `src/xgboost_hourly_load.py`

- Trains one XGBoost regressor per zone using lagged hourly loads, calendars, and weather features.
- Predicts the 24 hourly loads for a target date.
- Outputs CSV files under `src/predictions/xgboost_<DATE>.csv`.
- Cache directory: `src/model/xgboost_hourly` (override via `--model-dir`).

Usage example:

```bash
python src/xgboost_hourly_load.py \
  --train-pjm src/data/processed/pjm_training.csv \
  --train-weather src/data/processed/weather_training.csv \
  --eval-pjm src/data/processed/pjm_testing.csv \
  --eval-weather src/data/processed/weather_testing.csv \
  --date 2025-11-21
```

### Task B — Peak-Hour Classification

Script: `src/xgboost_peak_hour.py`

- Uses `HistGradientBoostingClassifier` per zone to classify which hour becomes the daily peak.
- Relies exclusively on historical load/weather features (no external forecasts).
- Saves predictions to `src/predictions/xgboost_peak_<DATE>.csv`.
- Cache directory: `src/model/xgboost_peak_hour`.

Usage example:

```bash
python src/xgboost_peak_hour.py \
  --train-pjm src/data/processed/pjm_training.csv \
  --train-weather src/data/processed/weather_training.csv \
  --eval-pjm src/data/processed/pjm_testing.csv \
  --eval-weather src/data/processed/weather_testing.csv \
  --date 2025-11-21
```

### Task C — Peak-Day Window Classification

Script: `src/predict_peak_window.py`

- Rolls the daily-peak model forward across a 10-day horizon (default window) while maintaining lagged history.
- Predicts the peak load for each zone/day in that window and reports whether the target date is within the top-4 days (I choose top 4 instead of top 2 because the loss function puts more weight on predicting actual peak day as the non-peak day).
- Outputs:
  - `src/predictions/window_peak_<START>_<END>.csv` with all zone/day predictions for the window.
  - `src/predictions/window_peak_summary_<TARGET>.csv` containing per-zone top-4 rankings and whether the target day qualifies.
- Internally relies on the daily-peak regression models saved under `src/model/xgboost_daily` (populated via `src/xgboost_daily_peak.py`).

Usage mirrors the other scripts, e.g.:

```bash
python src/predict_peak_window.py \
  --train-pjm src/data/processed/pjm_training.csv \
  --train-weather src/data/processed/weather_training.csv \
  --target-date 2025-11-21 \
  --start-date 2025-11-18 \
  --end-date 2025-11-27
```


## 3. Combined Prediction Entry Point

`src/make_predictions.py` coordinates all three tasks, prints one CSV-style line (date + 754 outputs), and appends it to `src/predictions/predictions.csv`. It supports:

- `--hourly-model-dir`, `--peak-hour-model-dir`, `--daily-model-dir` to override caches.
- `--force-retrain` to refresh all cached models for that run.
- Automatic data refresh/export when invoked via `make predictions` (quiet mode).

Example:

```bash
make predictions DATE=2025-11-21
```

This will:

1. Renew and export training data quietly.
2. Run `make_predictions.py` for the requested date.
3. Print only the final CSV line.


## 4. Notes & Tips

- The `DATE` Makefile variable defaults to `max(2025-11-20, tomorrow)` (America/New_York), ensuring contest constraints.
- Cached models live in `src/model/...`; `make clean` removes them, while `make clean-predictions` keeps models intact.
- Set `PJM_API_KEY` in your environment to override the default hard-coded key before running Make targets.