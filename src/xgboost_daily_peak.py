"""
 Predict zone-level daily peak load using per-day aggregates of weather signals
 combined with the previous three observed daily peaks.

Workflow:
    1. Merge processed PJM load data with hourly weather forecasts/observations.
    2. Aggregate the hourly records into (zone, date) rows that contain:
         • Daily max load (target).
         • Max/min/mean values for each weather column (temp, rhum, wind, etc.).
         • Calendar features (day of week, Thanksgiving flag).
         • Lagged peak-load features for the prior three days.
    3. Train an XGBoost regressor per zone and export predictions for the
       requested date.

Outputs:
    • predictions/daily_peak_<DATE>.csv with columns:
        zone, date, predicted_peak_load, actual_peak_load
    • Cached per-zone XGBoost models under model/xgboost_daily
      (override with --model-dir)

python xgboost_da.py \
  --train-pjm data/processed/pjm_training.csv \
  --train-weather data/processed/weather_training.csv \
  --eval-pjm data/processed/pjm_testing.csv \
  --eval-weather data/processed/weather_testing.csv \
  --date 2024-12-03 \
  --model-dir model/xgboost_daily \
  --force-retrain  # only when refreshing cached models
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import os
import re

import numpy as np
import pandas as pd
import pytz
from pandas.api.types import is_numeric_dtype

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("KMP_AFFINITY", "disabled")
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("KMP_INIT_AT_FORK", "FALSE")

from xgboost import XGBRegressor

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TRAIN_PJM = BASE_DIR / "data" / "processed" / "pjm_training.csv"
DEFAULT_TRAIN_WEATHER = BASE_DIR / "data" / "processed" / "weather_training.csv"
DEFAULT_EVAL_PJM = None
DEFAULT_EVAL_WEATHER = None
PREDICTION_DIR = BASE_DIR / "predictions"
DEFAULT_MODEL_DIR = BASE_DIR / "model" / "xgboost_daily"
PEAK_LAG_DAYS = 3
WEATHER_AGG_METRICS = ("mean", "max", "min")
MODEL_PARAMS = {
    "n_estimators": 500,
    "learning_rate": 0.05,
    "max_depth": 5,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "objective": "reg:squarederror",
    "tree_method": "hist",
    "random_state": 42,
    "n_jobs": -1,
}


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def default_prediction_date() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_ept = datetime.now(pytz.utc).astimezone(eastern)
    return (now_ept + timedelta(days=1)).strftime("%Y-%m-%d")


def load_split_frames(pjm_path: Path, weather_path: Path, split: str) -> pd.DataFrame:
    pjm = pd.read_csv(pjm_path)
    weather = pd.read_csv(weather_path)
    pjm["timestamp"] = pd.to_datetime(pjm["timestamp"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    weather["timestamp"] = pd.to_datetime(weather["timestamp"], errors="coerce")
    df = pd.merge(weather, pjm, on=["timestamp", "zone"], how="outer")
    df = df.dropna(subset=["timestamp", "zone"]).copy()
    df.sort_values(["zone", "timestamp"], inplace=True)
    df["split"] = split
    return df


def load_training_frames(pjm_path: Path, weather_path: Path) -> pd.DataFrame:
    return load_split_frames(pjm_path, weather_path, "train")


def thanksgiving_date(year: int) -> pd.Timestamp:
    nov1 = pd.Timestamp(year=year, month=11, day=1)
    first_thursday = nov1 + pd.Timedelta(days=(3 - nov1.weekday()) % 7)
    return (first_thursday + pd.Timedelta(weeks=3)).normalize()


def aggregate_daily_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = df["timestamp"].dt.normalize()
    reserved = {"timestamp", "zone", "load", "date", "split"}
    weather_cols = [
        col for col in df.columns if col not in reserved and is_numeric_dtype(df[col])
    ]

    agg_dict: dict[str, list[str]] = {"load": ["max"]}
    for col in weather_cols:
        agg_dict[col] = list(WEATHER_AGG_METRICS)

    grouped = df.groupby(["zone", "date", "split"]).agg(agg_dict)
    grouped.columns = [
        f"{col}_{stat}" if stat else col for col, stat in grouped.columns.to_flat_index()
    ]
    grouped = grouped.reset_index()
    grouped = grouped.rename(columns={"load_max": "peak_load"})
    return grouped


def add_calendar_features(daily_df: pd.DataFrame) -> None:
    daily_df["day_of_week"] = daily_df["date"].dt.dayofweek
    years = sorted({int(year) for year in daily_df["date"].dt.year.dropna().unique()})
    tg_map = {year: thanksgiving_date(year) for year in years}

    def mark_thanksgiving(day: pd.Timestamp | pd.NaT) -> bool:
        if pd.isna(day):
            return False
        tg = tg_map.get(day.year)
        if tg is None:
            return False
        return day in {tg - pd.Timedelta(days=1), tg, tg + pd.Timedelta(days=1)}

    daily_df["is_thanksgiving"] = daily_df["date"].apply(mark_thanksgiving)


def add_peak_lags(daily_df: pd.DataFrame, lag_days: int = PEAK_LAG_DAYS) -> list[str]:
    lag_cols: list[str] = []
    daily_df.sort_values(["zone", "date"], inplace=True)
    for day in range(1, lag_days + 1):
        col = f"lag_peak_{day}"
        daily_df[col] = daily_df.groupby("zone", sort=False)["peak_load"].shift(day)
        lag_cols.append(col)
    return lag_cols


def create_model() -> XGBRegressor:
    return XGBRegressor(**MODEL_PARAMS)


def sanitize_zone_name(zone: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", zone.strip().lower())
    return safe.strip("_") or "zone"


def get_zone_model_path(zone: str, model_dir: Path | None) -> Path | None:
    if model_dir is None:
        return None
    return model_dir / f"{sanitize_zone_name(zone)}.json"


def train_zone_model(
    zone: str,
    zone_df: pd.DataFrame,
    feature_cols: list[str],
    lag_cols: list[str],
    model_dir: Path | None,
    force_retrain: bool,
) -> tuple[XGBRegressor, np.ndarray]:
    model_path = get_zone_model_path(zone, model_dir)
    if model_path:
        ensure_dir(model_path.parent)
    model = create_model()
    if model_path and model_path.exists() and not force_retrain:
        model.load_model(model_path)
        print(f"[DAILY PEAK] Loaded cached model for zone {zone} from {model_path}")
    else:
        train_df = zone_df[zone_df["split"].eq("train")].dropna(subset=["peak_load"] + lag_cols)
        if train_df.empty:
            raise ValueError(f"No training data with lag history for zone {zone}")
        X = train_df[feature_cols]
        y = train_df["peak_load"]
        model.fit(X, y)
        if model_path:
            model.save_model(model_path)
            print(f"[DAILY PEAK] Saved model for zone {zone} to {model_path}")
    preds = model.predict(zone_df[feature_cols])
    return model, preds


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Predict zone-level daily peak load using aggregated weather features.")
    ap.add_argument("--train-pjm", type=Path, default=DEFAULT_TRAIN_PJM, help="Training PJM CSV path")
    ap.add_argument("--train-weather", type=Path, default=DEFAULT_TRAIN_WEATHER, help="Training weather CSV path")
    ap.add_argument("--eval-pjm", type=Path, default=DEFAULT_EVAL_PJM, help="Evaluation PJM CSV path (optional)")
    ap.add_argument("--eval-weather", type=Path, default=DEFAULT_EVAL_WEATHER, help="Evaluation weather CSV path (optional)")
    ap.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day EPT)")
    ap.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Directory to cache per-zone models (default: %(default)s)",
    )
    ap.add_argument(
        "--force-retrain",
        action="store_true",
        help="Retrain models even if cached versions exist",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    if bool(args.eval_pjm) != bool(args.eval_weather):
        raise ValueError("Provide both --eval-pjm and --eval-weather when using evaluation data.")
    target_date = pd.Timestamp(args.date).normalize()

    train_df = load_split_frames(args.train_pjm, args.train_weather, "train")
    frames = [train_df]
    target_split = "train"
    if args.eval_pjm and args.eval_weather:
        eval_df = load_split_frames(args.eval_pjm, args.eval_weather, "eval")
        frames.append(eval_df)
        target_split = "eval"

    hourly_df = pd.concat(frames, ignore_index=True)
    daily_df = aggregate_daily_features(hourly_df)
    add_calendar_features(daily_df)
    lag_cols = add_peak_lags(daily_df, PEAK_LAG_DAYS)

    reserved = {"zone", "date", "split", "peak_load", "day_of_week", "is_thanksgiving"}
    feature_cols = ["day_of_week", "is_thanksgiving"]
    weather_cols = [
        col for col in daily_df.columns if col not in reserved and col not in lag_cols and not col.startswith("lag_peak_")
    ]
    weather_cols.sort()
    feature_cols.extend(weather_cols + lag_cols)

    predictions: list[pd.DataFrame] = []
    for zone, zone_df in daily_df.groupby("zone", sort=False):
        zone_df = zone_df.sort_values("date").reset_index(drop=True)
        try:
            _, preds = train_zone_model(zone, zone_df, feature_cols, lag_cols, args.model_dir, args.force_retrain)
        except ValueError as exc:
            print(f"[DAILY PEAK] {exc}")
            continue
        zone_df["predicted_peak_load"] = preds
        target_rows = zone_df[zone_df["date"].eq(target_date) & zone_df["split"].eq(target_split)]
        if target_rows.empty:
            continue
        predictions.append(
            target_rows[["zone", "date", "predicted_peak_load", "peak_load"]].copy()
        )

    if not predictions:
        raise RuntimeError(
            "No predictions generated for the requested date. "
            "Ensure weather/PJM data includes hourly rows for that day."
        )

    final = pd.concat(predictions, ignore_index=True)
    final["date"] = final["date"].dt.strftime("%Y-%m-%d")
    final = final.rename(columns={"peak_load": "actual_peak_load"})
    if final["actual_peak_load"].notna().sum() == 0:
        final = final.drop(columns=["actual_peak_load"])
    ensure_dir(PREDICTION_DIR)
    out_path = PREDICTION_DIR / f"daily_peak_{target_date.strftime('%Y-%m-%d')}.csv"
    final.sort_values("zone").to_csv(out_path, index=False)
    print(f"[DAILY PEAK] Saved {len(final)} predictions for {target_date.date()} -> {out_path}")


if __name__ == "__main__":
    main()
