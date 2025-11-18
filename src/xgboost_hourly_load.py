"""
Train zone-level XGBoost models to predict hourly load for a specific date using PJM + weather data.

Features:
    • Previous 7 days of load for the same zone/hour (24h increments; NaNs allowed)
    • Hour of day encoded as sin/cos
    • Day of week (0=Mon)
    • Thanksgiving indicator
    • All weather features for the matching zone/timestamp

Outputs:
    • predictions/xgboost_<DATE>.csv with columns timestamp, zone, predicted_load, actual_load
    • Cached XGBoost models per zone in model/xgboost_hourly (override with --model-dir)

You can train on the default processed training CSVs while optionally supplying
separate PJM/weather evaluation CSVs (e.g., December “testing” files). When
evaluation files are provided the script still trains exclusively on the
training split but compares predictions against the evaluation PJM load.

python xgboost_hourly_load.py \
  --train-pjm data/processed/pjm_training.csv \
  --train-weather data/processed/weather_training.csv \
  --eval-pjm data/processed/pjm_testing.csv \
  --eval-weather data/processed/weather_testing.csv \
  --date 2024-12-03 \
  --model-dir model/xgboost_hourly \
  --force-retrain  # only when you need to refresh cached models

"""

from __future__ import annotations

import argparse
import math
import re
from datetime import datetime, timedelta
import pytz
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TRAIN_PJM = BASE_DIR / "data" / "processed" / "pjm_training.csv"
DEFAULT_TRAIN_WEATHER = BASE_DIR / "data" / "processed" / "weather_training.csv"
DEFAULT_EVAL_PJM = None
DEFAULT_EVAL_WEATHER = None
PREDICTION_DIR = BASE_DIR / "predictions"
DEFAULT_MODEL_DIR = BASE_DIR / "model" / "xgboost_hourly"
LAG_DAYS = 7
MODEL_PARAMS = {
    "n_estimators": 600,
    "learning_rate": 0.05,
    "max_depth": 7,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "objective": "reg:squarederror",
    "tree_method": "hist",
    "random_state": 42,
    "n_jobs": -1,
}


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_split_frames(pjm_path: Path, weather_path: Path, split: str) -> pd.DataFrame:
    pjm = pd.read_csv(pjm_path)
    weather = pd.read_csv(weather_path)

    pjm["timestamp"] = pd.to_datetime(pjm["timestamp"], errors="coerce")
    weather["timestamp"] = pd.to_datetime(weather["timestamp"], errors="coerce")
    df = pd.merge(weather, pjm, on=["timestamp", "zone"], how="outer")
    df = df.dropna(subset=["timestamp", "zone"]).copy()
    df = df.sort_values(["zone", "timestamp"])
    df["split"] = split
    return df


def thanksgiving_date(year: int) -> pd.Timestamp:
    nov1 = pd.Timestamp(year=year, month=11, day=1)
    # weekday: Monday=0; Thanksgiving is 4th Thursday
    first_thursday = nov1 + pd.Timedelta(days=(3 - nov1.weekday()) % 7)
    thanksgiving = first_thursday + pd.Timedelta(weeks=3)
    return thanksgiving.normalize()


def add_time_features(df: pd.DataFrame) -> None:
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["sin_hour"] = np.sin(2 * math.pi * df["hour"] / 24.0)
    df["cos_hour"] = np.cos(2 * math.pi * df["hour"] / 24.0)
    years = sorted({int(y) for y in df["timestamp"].dt.year.dropna().unique()})
    tg_map = {year: thanksgiving_date(year) for year in years}
    normalized = df["timestamp"].dt.normalize()

    def is_holiday(day: pd.Timestamp | pd.NaT) -> bool:
        if pd.isna(day):
            return False
        tg = tg_map.get(day.year)
        if tg is None:
            return False
        return day in {tg - pd.Timedelta(days=1), tg, tg + pd.Timedelta(days=1)}

    df["is_thanksgiving"] = normalized.apply(is_holiday)


def add_lag_features(df: pd.DataFrame, lag_days: int = LAG_DAYS) -> Iterable[str]:
    lag_cols = []
    df.sort_values(["zone", "timestamp"], inplace=True)
    for day in range(1, lag_days + 1):
        col = f"lag_day_{day}"
        df[col] = df.groupby("zone", sort=False)["load"].shift(24 * day)
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
    features: list[str],
    model_dir: Path | None,
    force_retrain: bool,
) -> tuple[XGBRegressor, np.ndarray]:
    model_path = get_zone_model_path(zone, model_dir)
    model = create_model()
    if model_path:
        ensure_dir(model_path.parent)
    if model_path and model_path.exists() and not force_retrain:
        model.load_model(model_path)
        print(f"[XGB] Loaded cached model for zone {zone} from {model_path}")
    else:
        train_rows = zone_df[zone_df["split"].eq("train")].dropna(subset=["load"])
        if train_rows.empty:
            raise ValueError(f"No training data available for zone {zone}")
        X = train_rows[features]
        y = train_rows["load"]
        model.fit(X, y)
        if model_path:
            model.save_model(model_path)
            print(f"[XGB] Saved model for zone {zone} to {model_path}")
    preds = model.predict(zone_df[features])
    return model, preds


def default_prediction_date() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_ept = datetime.now(pytz.utc).astimezone(eastern)
    target = (now_ept + timedelta(days=1)).date()
    return target.strftime("%Y-%m-%d")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Train XGBoost load model and export predictions.")
    ap.add_argument("--train-pjm", type=Path, default=DEFAULT_TRAIN_PJM, help="Training PJM CSV path")
    ap.add_argument("--train-weather", type=Path, default=DEFAULT_TRAIN_WEATHER, help="Training weather CSV path")
    ap.add_argument("--eval-pjm", type=Path, default=DEFAULT_EVAL_PJM, help="Evaluation PJM CSV path (optional)")
    ap.add_argument("--eval-weather", type=Path, default=DEFAULT_EVAL_WEATHER, help="Evaluation weather CSV path (optional)")
    ap.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day)")
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
        raise ValueError("Provide both --eval-pjm and --eval-weather when supplying evaluation data.")

    train_df = load_split_frames(args.train_pjm, args.train_weather, "train")
    frames = [train_df]
    target_split = "train"
    if args.eval_pjm and args.eval_weather:
        eval_df = load_split_frames(args.eval_pjm, args.eval_weather, "eval")
        frames.append(eval_df)
        target_split = "eval"

    df = pd.concat(frames, ignore_index=True).sort_values(["zone", "timestamp"])
    target_date = pd.Timestamp(args.date).normalize()
    add_time_features(df)
    lag_cols = list(add_lag_features(df, LAG_DAYS))
    reserved = {"timestamp", "zone", "load", "hour", "day_of_week", "sin_hour", "cos_hour", "is_thanksgiving", "split"}
    weather_cols = [c for c in df.columns if c not in reserved and not c.startswith("lag_day_")]
    feature_cols = ["sin_hour", "cos_hour", "day_of_week", "is_thanksgiving"] + weather_cols + lag_cols

    predictions = []
    for zone, zone_df in df.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp")
        try:
            _, zone_preds = train_zone_model(zone, zone_df, feature_cols, args.model_dir, args.force_retrain)
        except ValueError as exc:
            print(f"[WARN] {exc}")
            continue
        result = pd.DataFrame(
            {
                "timestamp": zone_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S"),
                "zone": zone,
                "predicted_load": zone_preds,
                "actual_load": zone_df["load"].values,
                "split": zone_df["split"].values,
            }
        )
        predictions.append(result)

    if not predictions:
        raise RuntimeError("No predictions produced; check training data.")
    final = pd.concat(predictions, ignore_index=True)
    times = pd.to_datetime(final["timestamp"])
    mask = times.dt.normalize().eq(target_date) & final["split"].eq(target_split)
    final = final.loc[mask].sort_values(["timestamp", "zone"])
    ensure_dir(PREDICTION_DIR)
    out_path = PREDICTION_DIR / f"xgboost_{target_date.strftime('%Y-%m-%d')}.csv"
    final.drop(columns=["split"]).to_csv(out_path, index=False)
    print(f"[XGB] Saved {len(final)} rows for target {target_date.date()} in {out_path} ({target_split} split)")


if __name__ == "__main__":
    main()
