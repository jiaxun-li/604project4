"""
Roll forward the daily XGBoost peak-load model across a 10-day window and tell
whether the target date lands inside the top-4 peak days for each zone.

Rules implemented:
    • The script trains the existing xgboost_daily_peak_model per zone.
    • Weather gaps are forward-filled so a missing forecast reuses the previous
      day's summary for that zone.
    • Missing lag peaks fall back to the most recent predictions; after a day is
      predicted its value becomes part of the history for future lags.
    • When an actual daily peak is present for a date, it is kept as-is and the
      model is not evaluated.

Outputs:
    predictions/window_peak_<START>_<END>.csv       — zone/day peak values
    predictions/window_peak_summary_<TARGET>.csv    — per-zone target rankings
    Cached models reused from model/xgboost_daily (override with --model-dir)

python predict_peak_window.py \
  --train-pjm data/processed/pjm_training.csv \
  --train-weather data/processed/weather_training.csv \
  --eval-pjm data/processed/pjm_testing.csv \
  --eval-weather data/processed/weather_testing.csv \
  --target-date 2024-12-03 \
  --start-date 2024-12-01 \
  --end-date 2024-12-10 \
  --model-dir model/xgboost_daily \
  --force-retrain
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from xgboost_daily_peak import (
    DEFAULT_TRAIN_PJM,
    DEFAULT_TRAIN_WEATHER,
    DEFAULT_EVAL_PJM,
    DEFAULT_EVAL_WEATHER,
    DEFAULT_MODEL_DIR,
    PREDICTION_DIR,
    PEAK_LAG_DAYS,
    add_calendar_features,
    add_peak_lags,
    aggregate_daily_features,
    default_prediction_date,
    ensure_dir,
    load_split_frames,
    train_zone_model,
)

DEFAULT_START = "2025-11-19"
DEFAULT_END = "2025-11-28"
TOP_K = 4


def normalize_date(value: str) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def default_target() -> str:
    return default_prediction_date()


def validate_window(
    start: pd.Timestamp, end: pd.Timestamp, target: pd.Timestamp, *, verbose: bool = True
) -> None:
    if start > end:
        raise ValueError("Start date must be before or equal to end date.")
    if not (start <= target <= end):
        raise ValueError("Target date must fall within the requested window.")
    if (end - start).days + 1 != 10 and verbose:
        print(f"[WARN] Window length is {(end - start).days + 1} days (expected 10).")


def expand_with_weather_fill(
    daily_df: pd.DataFrame, required_dates: pd.DatetimeIndex, weather_cols: list[str]
) -> pd.DataFrame:
    frames = []
    for zone, zone_df in daily_df.groupby("zone", sort=False):
        zone_df = zone_df.set_index("date").sort_index()
        reindexed = zone_df.reindex(zone_df.index.union(required_dates))
        reindexed["zone"] = zone
        if weather_cols:
            reindexed[weather_cols] = reindexed[weather_cols].ffill()
            reindexed[weather_cols] = reindexed[weather_cols].bfill()
        if "split" in reindexed.columns:
            reindexed["split"] = reindexed["split"].ffill().bfill()
        frames.append(reindexed.reset_index().rename(columns={"index": "date"}))
    expanded = pd.concat(frames, ignore_index=True)
    add_calendar_features(expanded)
    expanded.sort_values(["zone", "date"], inplace=True)
    return expanded


def build_feature_dict(
    zone_frame: pd.DataFrame, current_date: pd.Timestamp, weather_cols: list[str]
) -> dict[str, float] | None:
    if current_date not in zone_frame.index:
        return None
    row = zone_frame.loc[current_date]
    features: dict[str, float] = {
        "day_of_week": row["day_of_week"],
        "is_thanksgiving": float(row["is_thanksgiving"]),
    }
    for col in weather_cols:
        val = row.get(col)
        if pd.isna(val):
            return None
        features[col] = float(val)
    for lag in range(1, PEAK_LAG_DAYS + 1):
        lag_date = current_date - pd.Timedelta(days=lag)
        if lag_date not in zone_frame.index:
            return None
        lag_val = zone_frame.at[lag_date, "effective_peak"]
        if pd.isna(lag_val):
            return None
        features[f"lag_peak_{lag}"] = float(lag_val)
    return features


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Roll daily XGBoost peak predictions across a 10-day window.")
    ap.add_argument("--train-pjm", type=Path, default=DEFAULT_TRAIN_PJM, help="Training PJM CSV path")
    ap.add_argument("--train-weather", type=Path, default=DEFAULT_TRAIN_WEATHER, help="Training weather CSV path")
    ap.add_argument("--eval-pjm", type=Path, default=DEFAULT_EVAL_PJM, help="Evaluation PJM CSV path (optional)")
    ap.add_argument("--eval-weather", type=Path, default=DEFAULT_EVAL_WEATHER, help="Evaluation weather CSV path (optional)")
    ap.add_argument("--target-date", default=default_target(), help="Target date YYYY-MM-DD (default: tomorrow EPT)")
    ap.add_argument(
        "--start-date",
        default=DEFAULT_START,
        help=f"Window start date YYYY-MM-DD (default {DEFAULT_START})",
    )
    ap.add_argument(
        "--end-date",
        default=DEFAULT_END,
        help=f"Window end date YYYY-MM-DD (default {DEFAULT_END})",
    )
    ap.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Directory containing cached daily-peak models (default: %(default)s)",
    )
    ap.add_argument(
        "--force-retrain",
        action="store_true",
        help="Retrain models even if cached versions exist",
    )
    return ap.parse_args()


def compute_window_predictions(
    train_pjm: Path,
    train_weather: Path,
    eval_pjm: Path | None,
    eval_weather: Path | None,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    target_date: str | pd.Timestamp,
    model_dir: Path | None,
    force_retrain: bool,
    *,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if bool(eval_pjm) != bool(eval_weather):
        raise ValueError("Provide both eval PJM and weather paths or neither.")
    target_dt = normalize_date(str(target_date))
    start_dt = normalize_date(str(start_date))
    end_dt = normalize_date(str(end_date))
    validate_window(start_dt, end_dt, target_dt, verbose=verbose)
    window_dates = pd.date_range(start=start_dt, end=end_dt, freq="D")
    history_dates = pd.date_range(start=start_dt - pd.Timedelta(days=PEAK_LAG_DAYS), end=end_dt, freq="D")

    train_df = load_split_frames(train_pjm, train_weather, "train")
    frames = [train_df]
    target_split = "train"
    if eval_pjm and eval_weather:
        eval_df = load_split_frames(eval_pjm, eval_weather, "eval")
        frames.append(eval_df)
        target_split = "eval"

    hourly_df = pd.concat(frames, ignore_index=True)
    daily_df = aggregate_daily_features(hourly_df)
    add_calendar_features(daily_df)
    reserved = {"zone", "date", "split", "peak_load", "day_of_week", "is_thanksgiving"}
    weather_cols = [c for c in daily_df.columns if c not in reserved]
    weather_cols.sort()
    expanded_df = expand_with_weather_fill(daily_df, history_dates, weather_cols)
    lag_cols = add_peak_lags(expanded_df, PEAK_LAG_DAYS)
    feature_cols = ["day_of_week", "is_thanksgiving"] + weather_cols + lag_cols

    records: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []

    for zone, zone_df in expanded_df.groupby("zone", sort=False):
        zone_df = zone_df.sort_values("date").reset_index(drop=True)
        try:
            model, _ = train_zone_model(zone, zone_df, feature_cols, lag_cols, model_dir, force_retrain)
        except ValueError as exc:
            if verbose:
                print(f"[WINDOW] {exc}")
            continue

        zone_history = zone_df.set_index("date")
        train_mask = zone_history["split"].eq("train")
        zone_history["effective_peak"] = zone_history["peak_load"].where(train_mask)
        prior_mask = zone_history.index < start_dt
        prior_actuals = zone_history.loc[prior_mask & train_mask, "peak_load"].dropna()
        if not prior_actuals.empty:
            last_known_date = prior_actuals.index[-1]
            fill_end = start_dt - pd.Timedelta(days=1)
            if fill_end > last_known_date:
                fill_dates = pd.date_range(start=last_known_date + pd.Timedelta(days=1), end=fill_end, freq="D")
                for fill_date in fill_dates:
                    if fill_date not in zone_history.index:
                        continue
                    if not pd.isna(zone_history.at[fill_date, "effective_peak"]):
                        continue
                    feature_dict = build_feature_dict(zone_history, fill_date, weather_cols)
                    if feature_dict is None:
                        if verbose:
                            print(
                                f"[WINDOW] Unable to seed {zone} on {fill_date.date()} due to insufficient lag history."
                            )
                        break
                    feature_df = pd.DataFrame([feature_dict])[feature_cols]
                    prediction = float(model.predict(feature_df)[0])
                    zone_history.at[fill_date, "effective_peak"] = prediction
        zone_records = []
        for current_date in window_dates:
            if current_date not in zone_history.index:
                continue
            actual = zone_history.at[current_date, "peak_load"]
            actual_float = float(actual) if not pd.isna(actual) else None
            current_split = zone_history.at[current_date, "split"]
            use_actual = actual_float is not None and current_split == "train"
            if use_actual:
                zone_history.at[current_date, "effective_peak"] = actual
                record = {
                    "zone": zone,
                    "date": current_date,
                    "split": current_split,
                    "source": "actual",
                    "peak_value": float(actual),
                    "actual_peak_load": float(actual),
                    "predicted_peak_load": None,
                }
                zone_records.append(record)
                records.append(record)
                continue

            feature_dict = build_feature_dict(zone_history, current_date, weather_cols)
            if feature_dict is None:
                if verbose:
                    print(f"[WINDOW] Skipping {zone} {current_date.date()} — insufficient history.")
                continue
            feature_df = pd.DataFrame([feature_dict])[feature_cols]
            prediction = float(model.predict(feature_df)[0])
            zone_history.at[current_date, "effective_peak"] = prediction
            record = {
                "zone": zone,
                "date": current_date,
                "split": current_split,
                "source": "predicted",
                "peak_value": prediction,
                "actual_peak_load": actual_float,
                "predicted_peak_load": prediction,
            }
            zone_records.append(record)
            records.append(record)

        if not zone_records:
            continue
        zone_frame_full = pd.DataFrame(zone_records).sort_values("date")
        zone_frame = zone_frame_full[zone_frame_full["split"].eq(target_split)]
        target_row = zone_frame[zone_frame["date"].eq(target_dt)]
        if target_row.empty:
            continue
        zone_frame_sorted = zone_frame.sort_values("peak_value", ascending=False)
        top_k = zone_frame_sorted.head(TOP_K)
        summary_entry = {
            "zone": zone,
            "target_date": target_dt.strftime("%Y-%m-%d"),
            "target_peak_value": float(target_row["peak_value"].iloc[0]),
            "target_source": target_row["source"].iloc[0],
            "top_k_dates": ",".join(top_k["date"].dt.strftime("%Y-%m-%d")),
            "top_k_values": ",".join(top_k["peak_value"].round(2).astype(str)),
            "target_in_top_k": target_dt in set(top_k["date"]),
            "real_top_k_dates": "",
            "real_top_k_values": "",
        }

        real_window = zone_frame_full.dropna(subset=["actual_peak_load"])
        if not real_window.empty:
            real_sorted = real_window.sort_values("actual_peak_load", ascending=False).head(TOP_K)
            summary_entry["real_top_k_dates"] = ",".join(real_sorted["date"].dt.strftime("%Y-%m-%d"))
            summary_entry["real_top_k_values"] = ",".join(real_sorted["actual_peak_load"].round(2).astype(str))
        summaries.append(summary_entry)

    if not records:
        raise RuntimeError("No zone predictions produced for the requested window.")

    records_df = pd.DataFrame(records)
    records_df["date"] = records_df["date"].dt.strftime("%Y-%m-%d")
    summary_df = pd.DataFrame(summaries)
    return records_df.sort_values(["zone", "date"]), summary_df


def main() -> None:
    args = parse_args()
    records_df, summary_df = compute_window_predictions(
        args.train_pjm,
        args.train_weather,
        args.eval_pjm,
        args.eval_weather,
        args.start_date,
        args.end_date,
        args.target_date,
        args.model_dir,
        args.force_retrain,
    )
    start_date = normalize_date(args.start_date)
    end_date = normalize_date(args.end_date)
    target_date = normalize_date(args.target_date)
    ensure_dir(PREDICTION_DIR)
    window_path = PREDICTION_DIR / f"window_peak_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.csv"
    summary_path = PREDICTION_DIR / f"window_peak_summary_{target_date.strftime('%Y-%m-%d')}.csv"
    records_df.to_csv(window_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    print(f"[WINDOW] Saved {len(records_df)} zone-day rows → {window_path}")
    print(f"[WINDOW] Saved target-day rankings for {len(summary_df)} zones → {summary_path}")
    hits = summary_df["target_in_top_k"].sum()
    total = len(summary_df)
    print(f"[WINDOW] Target date appears in the top {TOP_K} for {hits}/{total} zones.")


if __name__ == "__main__":
    main()
