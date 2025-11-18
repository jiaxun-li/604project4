"""
Central entry point to produce the 754 contest predictions:
    • 29 zones × 24 hourly loads
    • 29 zone-level peak hours
    • 29 zone-level peak-day indicators (top-4 day in a 10-day window)

The script prints a single CSV-style line and also appends the same row to
predictions/predictions.csv without overwriting prior runs.


python make_predictions.py \
  --date 2024-12-03 \
  --pjm-csv data/processed/pjm_training.csv \
  --weather-csv data/processed/weather_training.csv \
  --eval-pjm data/processed/pjm_testing.csv \
  --eval-weather data/processed/weather_testing.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import warnings

import numpy as np
import pandas as pd
import pytz

from xgboost_hourly_load import (
    DEFAULT_TRAIN_PJM,
    DEFAULT_TRAIN_WEATHER,
    DEFAULT_EVAL_PJM,
    DEFAULT_EVAL_WEATHER,
    DEFAULT_MODEL_DIR as DEFAULT_HOURLY_MODEL_DIR,
    add_lag_features as add_hourly_lags,
    add_time_features as add_hourly_time_features,
    load_split_frames as load_hourly_split,
    train_zone_model as train_hourly_model,
)
from xgboost_peak_hour import (
    DEFAULT_MODEL_DIR as DEFAULT_PEAK_HOUR_MODEL_DIR,
    add_lag_features as add_peak_hour_lags,
    add_time_features as add_peak_hour_time_features,
    label_peak_hours,
    load_split_frames as load_peak_hour_split,
    summarise_history,
    train_zone_classifier,
)
from xgboost_daily_peak import DEFAULT_MODEL_DIR as DEFAULT_DAILY_MODEL_DIR
from predict_peak_window import compute_window_predictions

BASE_DIR = Path(__file__).resolve().parent
ZONE_COORDS = BASE_DIR / "config" / "zone_coords.json"
PREDICTION_LOG = BASE_DIR / "predictions" / "predictions.csv"
DEFAULT_START="2025-11-19"
DEFAULT_END="2025-11-28"

warnings.filterwarnings("ignore", message="Could not infer format.*", category=UserWarning)


def ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def default_prediction_date() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_ept = pd.Timestamp.now(tz=pytz.utc).astimezone(eastern)
    return (now_ept + pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate contest predictions (hourly load + peak hour + peak day).")
    ap.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day EPT)")
    ap.add_argument("--pjm-csv", type=Path, default=DEFAULT_TRAIN_PJM, help="Training PJM CSV path")
    ap.add_argument("--weather-csv", type=Path, default=DEFAULT_TRAIN_WEATHER, help="Training weather CSV path")
    ap.add_argument("--eval-pjm", type=Path, default=DEFAULT_EVAL_PJM, help="Evaluation PJM CSV path for hourly load (optional)")
    ap.add_argument("--eval-weather", type=Path, default=DEFAULT_EVAL_WEATHER, help="Evaluation weather CSV path (optional)")
    ap.add_argument("--start-day", default=DEFAULT_START, help="Start day for the period for predicting the peak days")
    ap.add_argument("--end-day", default=DEFAULT_END, help="End dat for the period for predicting the peak days")
    ap.add_argument(
        "--hourly-model-dir",
        type=Path,
        default=DEFAULT_HOURLY_MODEL_DIR,
        help="Cache directory for hourly load models (default: %(default)s)",
    )
    ap.add_argument(
        "--peak-hour-model-dir",
        type=Path,
        default=DEFAULT_PEAK_HOUR_MODEL_DIR,
        help="Cache directory for peak-hour classifiers (default: %(default)s)",
    )
    ap.add_argument(
        "--daily-model-dir",
        type=Path,
        default=DEFAULT_DAILY_MODEL_DIR,
        help="Cache directory for daily peak models (default: %(default)s)",
    )
    ap.add_argument(
        "--force-retrain",
        action="store_true",
        help="Retrain cached models/classifiers even if artifacts already exist",
    )
    return ap.parse_args()


def load_zone_order() -> list[str]:
    with ZONE_COORDS.open() as fh:
        data = json.load(fh)
    return list(data.keys())


def predict_hourly_loads(
    target_date: pd.Timestamp,
    train_pjm: Path,
    train_weather: Path,
    eval_pjm: Path | None,
    eval_weather: Path | None,
    model_dir: Path | None,
    force_retrain: bool,
) -> dict[str, list[float]]:
    if bool(eval_pjm) != bool(eval_weather):
        raise ValueError("Provide both --eval-pjm and --eval-weather or neither.")
    train_df = load_hourly_split(train_pjm, train_weather, "train")
    frames = [train_df]
    target_split = "train"
    if eval_pjm and eval_weather:
        eval_df = load_hourly_split(eval_pjm, eval_weather, "eval")
        frames.append(eval_df)
        target_split = "eval"
    df = pd.concat(frames, ignore_index=True).sort_values(["zone", "timestamp"])
    add_hourly_time_features(df)
    lag_cols = list(add_hourly_lags(df))
    reserved = {"timestamp", "zone", "load", "hour", "day_of_week", "sin_hour", "cos_hour", "is_thanksgiving", "split"}
    weather_cols = [c for c in df.columns if c not in reserved and not c.startswith("lag_day_")]
    feature_cols = ["sin_hour", "cos_hour", "day_of_week", "is_thanksgiving"] + weather_cols + lag_cols

    predictions: dict[str, list[float]] = {}
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    for zone, zone_df in df.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp")
        try:
            _, zone_preds = train_hourly_model(zone, zone_df, feature_cols, model_dir, force_retrain)
        except ValueError:
            continue
        zone_df = zone_df.assign(predicted_load=zone_preds)
        mask = zone_df["timestamp"].dt.normalize().eq(target_date) & zone_df["split"].eq(target_split)
        day_df = zone_df.loc[mask, ["timestamp", "predicted_load"]].sort_values("timestamp")
        if len(day_df) == 24:
            predictions[zone] = day_df["predicted_load"].tolist()
    return predictions


def predict_peak_hours(
    target_date: pd.Timestamp,
    train_pjm: Path,
    train_weather: Path,
    eval_pjm: Path | None,
    eval_weather: Path | None,
    model_dir: Path | None,
    force_retrain: bool,
) -> dict[str, int]:
    if bool(eval_pjm) != bool(eval_weather):
        raise ValueError("Provide both --eval-pjm and --eval-weather or neither.")
    train_df = load_peak_hour_split(train_pjm, train_weather, "train")
    frames = [train_df]
    target_split = "train"
    if eval_pjm and eval_weather:
        eval_df = load_peak_hour_split(eval_pjm, eval_weather, "eval")
        frames.append(eval_df)
        target_split = "eval"
    df = pd.concat(frames, ignore_index=True)
    add_peak_hour_time_features(df)
    lag_cols = list(add_peak_hour_lags(df))
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

    peak_hours: dict[str, int] = {}
    for zone, zone_df in df.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp").reset_index(drop=True)
        try:
            _, proba = train_zone_classifier(zone, zone_df, feature_cols, model_dir, force_retrain)
        except ValueError:
            continue
        zone_df["history_confidence"] = proba
        hist_summary = summarise_history(zone_df, zone)
        target_row = hist_summary[
            hist_summary["date"].eq(target_date) & hist_summary["split"].eq(target_split)
        ]
        if not target_row.empty:
            peak_hours[zone] = int(target_row["peak_hour_history"].iloc[0])
    return peak_hours


def predict_peak_day_flags(
    target_date: pd.Timestamp,
    train_pjm: Path,
    train_weather: Path,
    eval_pjm: Path | None,
    eval_weather: Path | None,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    model_dir: Path | None,
    force_retrain: bool,
) -> dict[str, int]:
    try:
        _, summary_df = compute_window_predictions(
            train_pjm,
            train_weather,
            eval_pjm,
            eval_weather,
            start_date=start_date,
            end_date=end_date,
            target_date=target_date,
            model_dir=model_dir,
            force_retrain=force_retrain,
            verbose=False,
        )
    except RuntimeError as exc:
        print(f"[WARN] Unable to compute peak-day window predictions: {exc}")
        return {}
    flags: dict[str, int] = {}
    for _, row in summary_df.iterrows():
        flags[row["zone"]] = 1 if bool(row["target_in_top_k"]) else 0
    return flags


def build_headers(zones: list[str]) -> list[str]:
    load_cols = [f"L{idx}_{hour:02d}" for idx in range(1, len(zones) + 1) for hour in range(24)]
    peak_hour_cols = [f"PH_{idx}" for idx in range(1, len(zones) + 1)]
    peak_day_cols = [f"PD_{idx}" for idx in range(1, len(zones) + 1)]
    return ["date"] + load_cols + peak_hour_cols + peak_day_cols


def build_output_row(
    date_str: str,
    zones: list[str],
    hourly: dict[str, list[float]],
    peak_hours: dict[str, int],
    peak_days: dict[str, int],
) -> tuple[list[str | int], str]:
    values: list[int] = []
    for zone in zones:
        zone_values = hourly.get(zone)
        if zone_values is None or len(zone_values) != 24:
            raise RuntimeError(f"Missing hourly predictions for zone {zone}")
        rounded = [int(np.rint(val)) for val in zone_values]
        values.extend(rounded)

    for zone in zones:
        peak_hour = int(peak_hours.get(zone, 0))
        values.append(peak_hour)

    for zone in zones:
        peak_day_flag = 1 if peak_days.get(zone, 0) else 0
        values.append(peak_day_flag)

    csv_row: list[str | int] = [date_str] + values
    console_line = ", ".join([f"\"{date_str}\""] + [str(v) for v in values])
    return csv_row, console_line


def append_to_csv(path: Path, header: list[str], row: list[str | int]) -> None:
    ensure_dir(path)
    file_exists = path.exists()
    with path.open("a", newline="") as fh:
        writer = csv.writer(fh)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


def main() -> None:
    args = parse_args()
    target_date = pd.Timestamp(args.date).normalize()
    zones = load_zone_order()

    hourly = predict_hourly_loads(
        target_date,
        args.pjm_csv,
        args.weather_csv,
        args.eval_pjm,
        args.eval_weather,
        args.hourly_model_dir,
        args.force_retrain,
    )
    if len(hourly) < len(zones):
        missing = set(zones) - set(hourly)
        if missing:
            raise RuntimeError(f"Missing hourly load predictions for zones: {', '.join(sorted(missing))}")
    peak_hours = predict_peak_hours(
        target_date,
        args.pjm_csv,
        args.weather_csv,
        args.eval_pjm,
        args.eval_weather,
        args.peak_hour_model_dir,
        args.force_retrain,
    )
    peak_days = predict_peak_day_flags(
        target_date,
        args.pjm_csv,
        args.weather_csv,
        args.eval_pjm,
        args.eval_weather,
        args.start_day,
        args.end_day,
        args.daily_model_dir,
        args.force_retrain,
    )

    row, console_line = build_output_row(target_date.strftime("%Y-%m-%d"), zones, hourly, peak_hours, peak_days)
    header = build_headers(zones)
    append_to_csv(PREDICTION_LOG, header, row)
    print(console_line)


if __name__ == "__main__":
    main()
