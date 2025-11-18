"""
History-based XGBoost classifier to predict next-day peak hour per PJM zone.

Uses hourly features (time-of-day, Thanksgiving flag, 7-day load lags, weather metrics)
to classify which hour is the daily peak. Only this classifier is used—no hourly load forecast.

Output: predictions/xgboost_peak_<DATE>.csv with columns:
    zone, date, predicted_peak_hour, predicted_timestamp, confidence, actual_peak_hour
"""

from __future__ import annotations

import argparse
import math
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytz
from xgboost import XGBClassifier

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("KMP_AFFINITY", "none")

BASE_DIR = Path(__file__).resolve().parent
PJM_CSV = BASE_DIR / "data" / "processed" / "pjm_training.csv"
WEATHER_CSV = BASE_DIR / "data" / "processed" / "weather_training.csv"
OUT_DIR = BASE_DIR / "predictions"
LAG_DAYS = 7
WEATHER_COLUMNS = ["temp", "dwpt", "rhum", "prcp", "wspd", "pres", "coco"]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def default_prediction_date() -> str:
    tz = pytz.timezone("US/Eastern")
    return (datetime.now(pytz.utc).astimezone(tz) + timedelta(days=1)).strftime("%Y-%m-%d")


def load_data() -> pd.DataFrame:
    pjm = pd.read_csv(PJM_CSV)
    weather = pd.read_csv(WEATHER_CSV)
    pjm["timestamp"] = pd.to_datetime(pjm["timestamp"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    weather["timestamp"] = pd.to_datetime(weather["timestamp"], errors="coerce")
    df = pd.merge(weather, pjm, on=["timestamp", "zone"], how="outer")
    df = df.dropna(subset=["timestamp", "zone"]).copy()
    df.sort_values(["zone", "timestamp"], inplace=True)
    return df


def thanksgiving_date(year: int) -> pd.Timestamp:
    nov1 = pd.Timestamp(year=year, month=11, day=1)
    first_thursday = nov1 + pd.Timedelta(days=(3 - nov1.weekday()) % 7)
    return (first_thursday + pd.Timedelta(weeks=3)).normalize()


def is_thanksgiving_day(day: pd.Timestamp | pd.NaT) -> bool:
    if pd.isna(day):
        return False
    tg = thanksgiving_date(day.year)
    d = day.normalize()
    return d in {tg - pd.Timedelta(days=1), tg, tg + pd.Timedelta(days=1)}


def add_time_features(df: pd.DataFrame) -> None:
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["sin_hour"] = np.sin(2 * math.pi * df["hour"] / 24.0)
    df["cos_hour"] = np.cos(2 * math.pi * df["hour"] / 24.0)
    df["date"] = df["timestamp"].dt.normalize()
    df["is_thanksgiving"] = df["timestamp"].apply(is_thanksgiving_day)


def add_load_lags(df: pd.DataFrame, lag_days: int = LAG_DAYS) -> None:
    df.sort_values(["zone", "timestamp"], inplace=True)
    for day in range(1, lag_days + 1):
        df[f"lag_day_{day}"] = df.groupby("zone", sort=False)["load"].shift(24 * day)


def label_peaks(df: pd.DataFrame) -> None:
    df["is_peak"] = False
    valid = df["load"].notna()
    peak_flags = (
        df.loc[valid]
        .groupby(["zone", "date"])["load"]
        .transform(lambda s: s == s.max())
    )
    df.loc[valid, "is_peak"] = peak_flags


def feature_columns(df: pd.DataFrame) -> list[str]:
    weather_cols = [col for col in WEATHER_COLUMNS if col in df.columns]
    lag_cols = [f"lag_day_{day}" for day in range(1, LAG_DAYS + 1)]
    return ["sin_hour", "cos_hour", "day_of_week", "is_thanksgiving"] + weather_cols + lag_cols


def train_classifier(zone_df: pd.DataFrame, features: list[str]) -> XGBClassifier | None:
    train_df = zone_df.dropna(subset=["load"])
    train_df = train_df.dropna(subset=features)
    if train_df.empty or train_df["is_peak"].sum() == 0:
        return None
    X = train_df[features]
    y = train_df["is_peak"].astype(int)
    model = XGBClassifier(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        tree_method="hist",
        random_state=42,
        n_jobs=1,
    )
    model.fit(X, y)
    return model


def predict_for_date(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    target_date = pd.Timestamp(date_str)
    features = feature_columns(df)
    results = []
    for zone, zone_df in df.groupby("zone"):
        model = train_classifier(zone_df, features)
        if model is None:
            continue
        target_rows = zone_df[zone_df["date"].eq(target_date)]
        target_rows = target_rows.dropna(subset=features)
        if target_rows.empty:
            continue
        probs = model.predict_proba(target_rows[features])[:, 1]
        best_idx = probs.argmax()
        best_row = target_rows.iloc[best_idx]
        predicted_hour = int(best_row["hour"])
        predicted_ts = best_row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        confidence = float(probs[best_idx])

        actual_rows = target_rows[target_rows["load"].notna()]
        actual_hour = (
            int(actual_rows.loc[actual_rows["load"].idxmax()]["hour"]) if not actual_rows.empty else None
        )

        results.append(
            {
                "zone": zone,
                "date": date_str,
                "predicted_peak_hour": predicted_hour,
                "predicted_timestamp": predicted_ts,
                "confidence": confidence,
                "actual_peak_hour": actual_hour,
            }
        )

    if not results:
        raise RuntimeError("No predictions generated; ensure weather data covers the target date.")
    return pd.DataFrame(results).sort_values("zone")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predict next-day peak hours using the history classifier.")
    parser.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_data()
    add_time_features(df)
    add_load_lags(df, LAG_DAYS)
    label_peaks(df)
    predictions = predict_for_date(df, args.date)
    ensure_dir(OUT_DIR)
    out_path = OUT_DIR / f"xgboost_peak_{args.date}.csv"
    predictions.to_csv(out_path, index=False)
    print(f"[PEAK] Saved {len(predictions)} zone predictions → {out_path}")


if __name__ == "__main__":
    main()
