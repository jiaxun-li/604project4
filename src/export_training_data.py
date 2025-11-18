"""
Export November PJM load and weather data for 2022–2025 into data/processed as the training data.

Outputs:
    data/processed/pjm_train.csv      (columns: timestamp, zone, load)
    data/processed/weather_train.csv  (same columns as source weather files)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
RAW_PJM = BASE_DIR / "data" / "raw" / "pjm"
RAW_WEATHER = BASE_DIR / "data" / "raw" / "weather"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

PJM_YEARS = [2022, 2023, 2024]
WEATHER_YEARS = [2022, 2023, 2024]
TARGET_MONTH = 11


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_pjm_november() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in PJM_YEARS:
        path = RAW_PJM / f"hrl_load_metered_{year}.csv"
        df = pd.read_csv(path)
        df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"], errors="coerce")
        df = df[df["datetime_beginning_ept"].dt.month.eq(TARGET_MONTH)]
        frames.append(df)
    nov_2025_path = RAW_PJM / "hrl_load_metered_2025_nov.csv"
    df_2025 = pd.read_csv(nov_2025_path)
    df_2025["datetime_beginning_ept"] = pd.to_datetime(df_2025["datetime_beginning_ept"], errors="coerce")
    frames.append(df_2025)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[["datetime_beginning_ept", "load_area", "mw"]].sort_values("datetime_beginning_ept")
    combined = combined[combined["load_area"].str.upper().ne("RTO")]
    combined["datetime_beginning_ept"] = combined["datetime_beginning_ept"].dt.strftime("%-m/%-d/%Y %-I:%M:%S %p")
    combined = combined.rename(
        columns={
            "datetime_beginning_ept": "timestamp",
            "load_area": "zone",
            "mw": "load",
        }
    )
    return combined


def load_weather_november() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in WEATHER_YEARS:
        path = RAW_WEATHER / f"weather_{year}.csv"
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df[df["timestamp"].dt.month.eq(TARGET_MONTH)]
        frames.append(df)
    path_2025 = RAW_WEATHER / "weather_2025_nov.csv"
    df_2025 = pd.read_csv(path_2025)
    df_2025["timestamp"] = pd.to_datetime(df_2025["timestamp"], errors="coerce")
    frames.append(df_2025)

    combined = pd.concat(frames, ignore_index=True).sort_values("timestamp")
    combined["timestamp"] = combined["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return combined


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Export November 2022-2025 PJM and weather data.")
    ap.add_argument("--outdir", type=Path, default=PROCESSED_DIR, help="Output directory (default data/processed)")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dir(args.outdir)
    pjm_df = load_pjm_november()
    weather_df = load_weather_november()
    pjm_path = args.outdir / "pjm_training.csv"
    weather_path = args.outdir / "weather_training.csv"
    pjm_df.to_csv(pjm_path, index=False)
    weather_df.to_csv(weather_path, index=False)
    print(f"Wrote {len(pjm_df):,} PJM rows to {pjm_path}")
    print(f"Wrote {len(weather_df):,} weather rows to {weather_path}")


if __name__ == "__main__":
    main()
