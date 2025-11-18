"""
Predict next-day peak hour per zone by combining:
    • Hourly XGBoost load forecasts (xgboost_model.py output)
    • A peak-hour classifier trained on prior peak-hour patterns
    • A logistic regression meta-model that chooses the better signal

Outputs: predictions/xgboost_peak_<DATE>.csv with columns:
    zone, date, peak_hour_load, peak_hour_history, combined_peak_hour,
    margin_norm, conf_history, load_prob, actual_peak_hour (if available)
"""

from __future__ import annotations

import argparse
import math
import os
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
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PJM = BASE_DIR / "data" / "processed" / "pjm_training.csv"
DEFAULT_WEATHER = BASE_DIR / "data" / "processed" / "weather_training.csv"
PREDICTION_DIR = BASE_DIR / "predictions"
HOURLY_PRED_PATTERN = "xgboost_{date}.csv"
LAG_DAYS = 7


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def default_prediction_date() -> str:
    eastern = pytz.timezone("US/Eastern")
    now_ept = datetime.now(pytz.utc).astimezone(eastern)
    return (now_ept + timedelta(days=1)).strftime("%Y-%m-%d")


def load_training_frames(pjm_path: Path, weather_path: Path) -> pd.DataFrame:
    pjm = pd.read_csv(pjm_path)
    weather = pd.read_csv(weather_path)
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


def train_zone_classifier(zone_df: pd.DataFrame, feature_cols: list[str]) -> tuple[HistGradientBoostingClassifier, np.ndarray]:
    train_df = zone_df.dropna(subset=["load"])
    if train_df["is_peak"].sum() == 0:
        raise ValueError(f"No peak labels available for zone {zone_df['zone'].iloc[0]}")

    X = train_df[feature_cols]
    y = train_df["is_peak"].astype(int)
    model = HistGradientBoostingClassifier(
        max_iter=300,
        learning_rate=0.05,
        max_depth=6,
        l2_regularization=1e-4,
        random_state=42,
    )
    model.fit(X, y)
    proba = model.predict_proba(zone_df[feature_cols])[:, 1]
    return model, proba


def compute_peak_from_series(series: pd.Series, hours: pd.Series) -> tuple[int | None, float | None]:
    values = series.to_numpy(dtype=float)
    hours_arr = hours.to_numpy(dtype=float)
    mask = ~np.isnan(values)
    if not mask.any():
        return None, None
    idx = np.argsort(values[mask])[::-1]
    top_hour = int(hours_arr[mask][idx[0]])
    top_value = values[mask][idx[0]]

    second_value = np.nan
    for candidate in idx[1:]:
        cand_hour = hours_arr[mask][candidate]
        if abs(cand_hour - top_hour) > 1:
            second_value = values[mask][candidate]
            break
    if np.isnan(second_value):
        second_value = 0.0
    margin = top_value - second_value
    margin_norm = margin / (abs(top_value) + 1e-6)
    return top_hour, margin_norm


def summarise_history(zone_df: pd.DataFrame, zone: str) -> pd.DataFrame:
    records = []
    for date, sub in zone_df.groupby("date"):
        if sub.empty:
            continue
        idx = sub["history_confidence"].idxmax()
        row = sub.loc[idx]
        top_hour = int(row["hour"])
        mask_neighbors = sub["hour"].sub(top_hour).abs().le(1)
        conf_sum = float(sub.loc[mask_neighbors, "history_confidence"].sum())
        conf_sum = min(conf_sum, 1.0)
        records.append(
            {
                "zone": zone,
                "date": date,
                "peak_hour_history": top_hour,
                "conf_history": conf_sum,
            }
        )
    return pd.DataFrame(records)


def summarise_load(zone_df: pd.DataFrame, value_col: str, zone: str, actual: bool = True) -> pd.DataFrame:
    records = []
    by_date = zone_df.groupby("date")
    for date, sub in by_date:
        peak_hour, margin = compute_peak_from_series(sub[value_col], sub["hour"])
        if peak_hour is None:
            continue
        record = {
            "zone": zone,
            "date": date,
            "peak_hour_load": peak_hour,
            "margin_norm": margin,
        }
        if actual and sub["load"].notna().any():
            actual_peak = int(sub.loc[sub["load"].idxmax()]["hour"])
            record["actual_peak_hour"] = actual_peak
        records.append(record)
    return pd.DataFrame(records)


def load_hourly_predictions(target_date: pd.Timestamp, hourly_csv: Path | None) -> pd.DataFrame:
    if hourly_csv is None:
        hourly_csv = PREDICTION_DIR / HOURLY_PRED_PATTERN.format(date=target_date.strftime("%Y-%m-%d"))
    if not hourly_csv.exists():
        raise FileNotFoundError(f"Hourly prediction CSV not found: {hourly_csv}. Run xgboost_model.py first.")
    df = pd.read_csv(hourly_csv)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = df["timestamp"].dt.normalize()
    df["hour"] = df["timestamp"].dt.hour
    target_df = df[df["date"].eq(target_date)].copy()
    if target_df.empty:
        raise ValueError(f"No hourly predictions found for {target_date.date()} in {hourly_csv}.")
    return target_df


def build_meta_training_data(df: pd.DataFrame, history_preds: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for zone, zone_df in df.groupby("zone"):
        hist = history_preds.get(zone)
        if hist is None or hist.empty:
            continue
        load_summary = summarise_load(zone_df, "load", zone, actual=True)
        merged = pd.merge(hist, load_summary, on="date", how="inner")
        if merged.empty:
            continue
        merged = merged.dropna(subset=["actual_peak_hour"])
        if merged.empty:
            continue
        merged = merged[merged["peak_hour_load"].ne(merged["peak_hour_history"])]
        if merged.empty:
            continue
        merged["label"] = merged.apply(
            lambda row: int(
                abs(row["actual_peak_hour"] - row["peak_hour_load"])
                <= abs(row["actual_peak_hour"] - row["peak_hour_history"])
            ),
            axis=1,
        )
        merged["zone"] = zone
        rows.append(merged)
    if not rows:
        raise RuntimeError("No data available to train logistic meta-model.")
    meta = pd.concat(rows, ignore_index=True)
    meta["margin_norm"] = meta["margin_norm"].fillna(0.0)
    meta["conf_history"] = meta["conf_history"].fillna(0.0)
    return meta[["zone", "date", "margin_norm", "conf_history", "label"]]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Predict next-day peak hour per zone using ensemble.")
    ap.add_argument("--pjm-csv", type=Path, default=DEFAULT_PJM, help="Processed PJM training CSV path")
    ap.add_argument("--weather-csv", type=Path, default=DEFAULT_WEATHER, help="Processed weather CSV path")
    ap.add_argument("--date", default=default_prediction_date(), help="Target date YYYY-MM-DD (default: next day EPT)")
    ap.add_argument(
        "--hourly-pred-csv",
        type=Path,
        default=None,
        help="Optional path to xgboost hourly prediction CSV (default: predictions/xgboost_<DATE>.csv)",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    target_date = pd.Timestamp(args.date).normalize()

    df = load_training_frames(args.pjm_csv, args.weather_csv)
    add_time_features(df)
    lag_cols = list(add_lag_features(df, LAG_DAYS))
    label_peak_hours(df)

    reserved = {"timestamp", "zone", "load", "hour", "day_of_week", "sin_hour", "cos_hour", "is_thanksgiving", "date", "is_peak"}
    feature_cols = ["sin_hour", "cos_hour", "day_of_week", "is_thanksgiving"] + lag_cols
    weather_cols = [c for c in df.columns if c not in reserved and not c.startswith("lag_day_")]
    feature_cols.extend(weather_cols)

    history_predictions: dict[str, pd.DataFrame] = {}
    history_target_rows = []
    for zone, zone_df in df.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp").reset_index(drop=True)
        try:
            _, proba = train_zone_classifier(zone_df, feature_cols)
        except ValueError as exc:
            print(f"[PEAK] {exc}")
            continue
        zone_df["history_confidence"] = proba
        hist_summary = summarise_history(zone_df, zone)
        history_predictions[zone] = hist_summary
        target_row = hist_summary[hist_summary["date"].eq(target_date)]
        if not target_row.empty:
            history_target_rows.append((zone, target_row.iloc[0]))

    meta_training = build_meta_training_data(df, history_predictions)
    feature_cols_meta = ["margin_norm", "conf_history"]
    feature_matrix = meta_training[feature_cols_meta]
    labels = meta_training["label"]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(feature_matrix)
    meta_model = SGDClassifier(loss="log_loss", max_iter=2000, tol=1e-4)
    meta_model.fit(X_scaled, labels)

    hourly_preds = load_hourly_predictions(target_date, args.hourly_pred_csv)
    load_features = []
    for zone, zone_df in hourly_preds.groupby("zone"):
        summary = summarise_load(zone_df, "predicted_load", zone, actual=False)
        if summary.empty:
            continue
        row = summary.iloc[0]
        load_features.append(
            {
                "zone": zone,
                "peak_hour_load": row["peak_hour_load"],
                "margin_norm": row["margin_norm"],
            }
        )
    load_features_df = pd.DataFrame(load_features)
    if load_features_df.empty:
        raise RuntimeError("No load-based peak signals available.")

    history_target_df = pd.DataFrame(
        [
            {
                "zone": zone,
                "peak_hour_history": row["peak_hour_history"],
                "conf_history": row["conf_history"],
            }
            for zone, row in history_target_rows
        ]
    )

    combined = pd.merge(load_features_df, history_target_df, on="zone", how="inner")
    probs = meta_model.predict_proba(scaler.transform(combined[feature_cols_meta]))[:, 1]
    combined["load_prob"] = probs
    combined["combined_peak_hour"] = np.where(
        combined["load_prob"] >= 0.5, combined["peak_hour_load"], combined["peak_hour_history"]
    )

    actual_rows = []
    for zone, zone_df in df[df["date"].eq(target_date)].groupby("zone"):
        summary = summarise_load(zone_df, "load", zone, actual=True)
        if summary.empty or "actual_peak_hour" not in summary:
            continue
        actual_rows.append(summary[["zone", "date", "actual_peak_hour"]])
    actual_summary = (
        pd.concat(actual_rows, ignore_index=True)
        if actual_rows
        else pd.DataFrame(columns=["zone", "actual_peak_hour"])
    )
    combined = combined.merge(actual_summary[["zone", "actual_peak_hour"]], on="zone", how="left")
    combined["date"] = target_date.strftime("%Y-%m-%d")

    ensure_dir(PREDICTION_DIR)
    out_path = PREDICTION_DIR / f"xgboost_peak_{target_date.strftime('%Y-%m-%d')}.csv"
    combined.sort_values("zone").to_csv(out_path, index=False)
    print(f"[PEAK] Saved combined peak-hour predictions for {len(combined)} zones → {out_path}")


if __name__ == "__main__":
    main()