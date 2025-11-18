"""
Train zone-level XGBoost models to predict hourly load for a specific date using PJM + weather data.

Features:
    • Previous 7 days of load for the same zone/hour (24h increments; NaNs allowed)
    • Hour of day encoded as sin/cos
    • Day of week (0=Mon)
    • Thanksgiving indicator
    • All weather features for the matching zone/timestamp

Outputs:
    predictions/xgboost_<DATE>.csv with columns timestamp, zone, predicted_load, actual_load
"""

from __future__ import annotations

import argparse
import math
from datetime import datetime, timedelta
import pytz
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PJM = BASE_DIR / "data" / "processed" / "pjm_training.csv"
DEFAULT_WEATHER = BASE_DIR / "data" / "processed" / "weather_training.csv"
PREDICTION_DIR = BASE_DIR / "predictions"
LAG_DAYS = 7


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_training_frames(pjm_path: Path, weather_path: Path) -> pd.DataFrame:
    pjm = pd.read_csv(pjm_path)
    weather = pd.read_csv(weather_path)

    pjm["timestamp"] = pd.to_datetime(pjm["timestamp"], errors="coerce")
    weather["timestamp"] = pd.to_datetime(weather["timestamp"], errors="coerce")
    df = pd.merge(weather, pjm, on=["timestamp", "zone"], how="outer")
    df = df.dropna(subset=["timestamp", "zone"]).copy()
    df = df.sort_values(["zone", "timestamp"])
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
    df["is_thanksgiving"] = normalized.apply(lambda d: bool(pd.notna(d) and d == tg_map.get(d.year)))


def add_lag_features(df: pd.DataFrame, lag_days: int = LAG_DAYS) -> Iterable[str]:
    lag_cols = []
    df.sort_values(["zone", "timestamp"], inplace=True)
    for day in range(1, lag_days + 1):
        col = f"lag_day_{day}"
        df[col] = df.groupby("zone", sort=False)["load"].shift(24 * day)
        lag_cols.append(col)
    return lag_cols


def train_zone_model(zone_df: pd.DataFrame, features: list[str]) -> tuple[XGBRegressor, np.ndarray]:
    valid_rows = zone_df.dropna(subset=["load"])
    if valid_rows.empty:
        raise ValueError(f"No training data available for zone {zone_df['zone'].iloc[0]}")
    X = valid_rows[features]
    y = valid_rows["load"]
    model = XGBRegressor(
        n_estimators=600,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.9,
        objective="reg:squarederror",
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X, y)
    preds = model.predict(zone_df[features])
    return model, preds


def default_prediction_date() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_ept = datetime.now(pytz.utc).astimezone(eastern)
    target = (now_ept + timedelta(days=1)).date()
    return target.strftime("%Y-%m-%d")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Train XGBoost load model and export predictions.")
    ap.add_argument("--pjm-csv", type=Path, default=DEFAULT_PJM, help="Processed PJM training CSV path")
    ap.add_argument("--weather-csv", type=Path, default=DEFAULT_WEATHER, help="Processed weather training CSV path")
    ap.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day)")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    df = load_training_frames(args.pjm_csv, args.weather_csv)
    target_date = pd.Timestamp(args.date).normalize()
    add_time_features(df)
    lag_cols = list(add_lag_features(df, LAG_DAYS))
    reserved = {"timestamp", "zone", "load", "hour", "day_of_week", "sin_hour", "cos_hour", "is_thanksgiving"}
    weather_cols = [c for c in df.columns if c not in reserved and not c.startswith("lag_day_")]
    feature_cols = ["sin_hour", "cos_hour", "day_of_week", "is_thanksgiving"] + weather_cols + lag_cols

    predictions = []
    for zone, zone_df in df.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp")
        try:
            _, zone_preds = train_zone_model(zone_df, feature_cols)
        except ValueError as exc:
            print(f"[WARN] {exc}")
            continue
        result = pd.DataFrame(
            {
                "timestamp": zone_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S"),
                "zone": zone,
                "predicted_load": zone_preds,
                "actual_load": zone_df["load"].values,
            }
        )
        predictions.append(result)

    if not predictions:
        raise RuntimeError("No predictions produced; check training data.")
    final = pd.concat(predictions, ignore_index=True)
    mask = pd.to_datetime(final["timestamp"]).dt.normalize().eq(target_date)
    final = final.loc[mask].sort_values(["timestamp", "zone"])
    ensure_dir(PREDICTION_DIR)
    out_path = PREDICTION_DIR / f"xgboost_{target_date.strftime('%Y-%m-%d')}.csv"
    final.to_csv(out_path, index=False)
    print(f"[XGB] Saved {len(final)} rows for target {target_date.date()} in {out_path}")


if __name__ == "__main__":
    main()
