"""
Predict next-day peak hour per zone using a single signal: a
HistGradientBoostingClassifier trained on prior peak-hour patterns.  The script
scores every (zone, hour) combination that exists in the merged weather + PJM
dataset, aggregates the per-hour probabilities into a single peak-hour
prediction, and writes predictions/xgboost_peak_<DATE>.csv containing:

    zone, date, peak_hour_history, conf_history
    Cached per-zone models saved under model/xgboost_peak_hour (override via --model-dir)

Only historical patterns are used. No hourly load forecasts or meta-models are
required anymore.

python xgboost_peak_hour.py \
  --train-pjm data/processed/pjm_training.csv \
  --train-weather data/processed/weather_training.csv \
  --eval-pjm data/processed/pjm_testing.csv \
  --eval-weather data/processed/weather_testing.csv \
  --date 2024-12-03 \
  --model-dir model/xgboost_peak_hour \
  --force-retrain

"""

from __future__ import annotations

import argparse
import math
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("KMP_AFFINITY", "disabled")
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("KMP_INIT_AT_FORK", "FALSE")

import numpy as np
import pandas as pd
import pytz
from joblib import dump, load
from sklearn.ensemble import HistGradientBoostingClassifier

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TRAIN_PJM = BASE_DIR / "data" / "processed" / "pjm_training.csv"
DEFAULT_TRAIN_WEATHER = BASE_DIR / "data" / "processed" / "weather_training.csv"
DEFAULT_EVAL_PJM = None
DEFAULT_EVAL_WEATHER = None
PREDICTION_DIR = BASE_DIR / "predictions"
DEFAULT_MODEL_DIR = BASE_DIR / "model" / "xgboost_peak_hour"
LAG_DAYS = 7
MODEL_PARAMS = {
    "max_iter": 300,
    "learning_rate": 0.05,
    "max_depth": 6,
    "l2_regularization": 1e-4,
    "random_state": 42,
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
    df["date"] = normalized


def add_lag_features(df: pd.DataFrame, lag_days: int = LAG_DAYS) -> Iterable[str]:
    lag_cols = []
    for day in range(1, lag_days + 1):
        col = f"lag_day_{day}"
        df[col] = df.groupby("zone", sort=False)["load"].shift(24 * day)
        lag_cols.append(col)
    return lag_cols


def label_peak_hours(df: pd.DataFrame) -> None:
    df["is_peak"] = False
    valid = df["load"].notna()
    peak_flags = (
        df.loc[valid]
        .groupby(["zone", "date"])["load"]
        .transform(lambda s: s == s.max())
    )
    df.loc[valid, "is_peak"] = peak_flags


def create_classifier() -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(**MODEL_PARAMS)


def sanitize_zone_name(zone: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", zone.strip().lower())
    return safe.strip("_") or "zone"


def get_zone_model_path(zone: str, model_dir: Path | None) -> Path | None:
    if model_dir is None:
        return None
    return model_dir / f"{sanitize_zone_name(zone)}.joblib"


def train_zone_classifier(
    zone: str,
    zone_df: pd.DataFrame,
    feature_cols: list[str],
    model_dir: Path | None,
    force_retrain: bool,
) -> tuple[HistGradientBoostingClassifier, np.ndarray]:
    model_path = get_zone_model_path(zone, model_dir)
    model: HistGradientBoostingClassifier
    if model_path:
        ensure_dir(model_path.parent)
    if model_path and model_path.exists() and not force_retrain:
        model = load(model_path)
        print(f"[PEAK] Loaded cached classifier for zone {zone} from {model_path}")
    else:
        train_df = zone_df[zone_df["split"].eq("train")].dropna(subset=["load"])
        if train_df["is_peak"].sum() == 0:
            raise ValueError(f"No peak labels available for zone {zone}")
        X = train_df[feature_cols]
        y = train_df["is_peak"].astype(int)
        model = create_classifier()
        model.fit(X, y)
        if model_path:
            dump(model, model_path)
            print(f"[PEAK] Saved classifier for zone {zone} to {model_path}")
    proba = model.predict_proba(zone_df[feature_cols])[:, 1]
    return model, proba


def summarise_history(zone_df: pd.DataFrame, zone: str) -> pd.DataFrame:
    records = []
    for (date, split), sub in zone_df.groupby(["date", "split"]):
        if sub.empty:
            continue
        idx = sub["history_confidence"].idxmax()
        row = sub.loc[idx]
        top_hour = int(row["hour"])
        mask_neighbors = sub["hour"].sub(top_hour).abs().le(1)
        conf_sum = float(sub.loc[mask_neighbors, "history_confidence"].sum())
        conf_sum = min(conf_sum, 1.0)
        actual_peak = pd.NA
        actual_rows = sub[sub["is_peak"]]
        if not actual_rows.empty:
            actual_peak = int(actual_rows["hour"].iloc[0])

        records.append(
            {
                "zone": zone,
                "date": date,
                "split": split,
                "peak_hour_history": top_hour,
                "conf_history": conf_sum,
                "actual_peak_hour": actual_peak,
            }
        )
    return pd.DataFrame(records)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Predict next-day peak hour per zone using historical patterns only.")
    ap.add_argument("--train-pjm", type=Path, default=DEFAULT_TRAIN_PJM, help="Training PJM CSV path")
    ap.add_argument("--train-weather", type=Path, default=DEFAULT_TRAIN_WEATHER, help="Training weather CSV path")
    ap.add_argument("--eval-pjm", type=Path, default=DEFAULT_EVAL_PJM, help="Evaluation PJM CSV path (optional)")
    ap.add_argument("--eval-weather", type=Path, default=DEFAULT_EVAL_WEATHER, help="Evaluation weather CSV path (optional)")
    ap.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day EPT)")
    ap.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Directory to cache/load per-zone classifiers (default: %(default)s)",
    )
    ap.add_argument(
        "--force-retrain",
        action="store_true",
        help="Retrain classifiers even if cached versions exist",
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

    df = pd.concat(frames, ignore_index=True)
    add_time_features(df)
    lag_cols = list(add_lag_features(df, LAG_DAYS))
    label_peak_hours(df)

    reserved = {
        "timestamp",
        "zone",
        "split",
        "load",
        "hour",
        "day_of_week",
        "sin_hour",
        "cos_hour",
        "is_thanksgiving",
        "date",
        "is_peak",
    }
    feature_cols = ["sin_hour", "cos_hour", "day_of_week", "is_thanksgiving"] + lag_cols
    weather_cols = [c for c in df.columns if c not in reserved and not c.startswith("lag_day_")]
    feature_cols.extend(weather_cols)

    history_rows = []
    for zone, zone_df in df.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp").reset_index(drop=True)
        try:
            _, proba = train_zone_classifier(zone, zone_df, feature_cols, args.model_dir, args.force_retrain)
        except ValueError as exc:
            print(f"[PEAK] {exc}")
            continue
        zone_df["history_confidence"] = proba
        hist_summary = summarise_history(zone_df, zone)
        target_row = hist_summary[
            hist_summary["date"].eq(target_date) & hist_summary["split"].eq(target_split)
        ]
        if target_row.empty:
            continue
        row_dict = target_row.iloc[0].to_dict()
        if "actual_peak_hour" in row_dict and pd.isna(row_dict["actual_peak_hour"]):
            row_dict["actual_peak_hour"] = None
        history_rows.append(row_dict)

    if not history_rows:
        raise RuntimeError(
            "No historical peak-hour predictions available for the requested date. "
            "Ensure the weather/PJM CSVs contain rows for that date."
        )

    history_df = pd.DataFrame(history_rows)
    if "split" in history_df:
        history_df = history_df.drop(columns=["split"])
    history_df["date"] = history_df["date"].dt.strftime("%Y-%m-%d")

    ensure_dir(PREDICTION_DIR)
    out_path = PREDICTION_DIR / f"xgboost_peak_{target_date.strftime('%Y-%m-%d')}.csv"
    history_df.sort_values("zone").to_csv(out_path, index=False)
    print(f"[PEAK] Saved historical peak-hour predictions for {len(history_df)} zones → {out_path}")


if __name__ == "__main__":
    main()
