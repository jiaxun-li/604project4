"""
Renew both weather and PJM HRL load datasets for a given month (defaults to Nov 2025).

Example:
    export PJM_API_KEY=...
    python renew_data.py --zones-json config/zone_coords.json
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd

from pjm_download import (
    DEFAULT_DATASET as PJM_DEFAULT_DATASET,
    DEFAULT_ENV_KEY as PJM_DEFAULT_ENV,
    default_output_path as pjm_default_output_path,
    download_month as download_pjm_month,
    normalize_csv_datetimes as normalize_pjm_csv,
)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def month_bounds(year: int, month: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(0)
    return start, end + pd.Timedelta(days=1)  # Meteostat end is exclusive


def weather_out_path(year: int, month: int) -> Path:
    mon = pd.Timestamp(year=year, month=month, day=1).strftime("%b").lower()
    return Path("data/raw/weather") / f"weather_{year}_{mon}.csv"


def download_weather_month(zones_json: Path, year: int, month: int, out_csv: Path) -> None:
    try:
        from meteostat import Point, Hourly
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Please install Meteostat first: `pip install meteostat`") from exc

    with zones_json.open("r") as handle:
        zone_coords = json.load(handle)

    ensure_dir(out_csv.parent)
    start, end_excl = month_bounds(year, month)

    frames: list[pd.DataFrame] = []
    print(f"[WX] {start.date()} → {(end_excl - pd.Timedelta(days=1)).date()} for {len(zone_coords)} zones")
    for zone, coords in zone_coords.items():
        try:
            lat, lon = float(coords[0]), float(coords[1])
            loc = Point(lat, lon)
            data = Hourly(loc, start, end_excl).fetch()
            if data is None or data.empty:
                print(f"  ! {zone}: no data")
                continue
            df = data.reset_index().rename(columns={"time": "timestamp"})
            df["zone"] = str(zone)
            keep_cols = ["timestamp", "zone"] + [
                col for col in ["temp", "dwpt", "rhum", "prcp", "wspd", "pres", "coco"] if col in df.columns
            ]
            frames.append(df[keep_cols])
            print(f"  ✓ {zone}: {len(df)} rows")
            time.sleep(0.15)
        except Exception as err:  # pragma: no cover
            print(f"  ! {zone}: {err}")

    if not frames:
        raise RuntimeError("No weather retrieved.")
    out_df = pd.concat(frames, ignore_index=True).sort_values(["zone", "timestamp"])
    out_df.to_csv(out_csv, index=False)
    print(f"[WX] Wrote {len(out_df):,} rows → {out_csv}")


def download_pjm(args) -> Path:
    api_key = os.getenv(args.pjm_api_key_env)
    if not api_key:
        raise RuntimeError(f"Missing API key in ${args.pjm_api_key_env}. Set it before running.")
    out_path = args.pjm_out or pjm_default_output_path(args.pjm_dataset, args.year, args.month)
    csv_path = download_pjm_month(args.pjm_dataset, api_key, args.year, args.month, Path(out_path), args.pjm_row_count)
    return normalize_pjm_csv(csv_path)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Renew weather data and PJM load data for a month.")
    ap.add_argument("--zones-json", type=Path, required=True, help="Path to zone_coords.json")
    ap.add_argument("--year", type=int, default=2025, help="Target year (default 2025)")
    ap.add_argument("--month", type=int, default=11, help="Target month 1–12 (default 11)")
    ap.add_argument("--skip-weather", action="store_true", help="Skip downloading weather data")
    ap.add_argument("--skip-pjm", action="store_true", help="Skip downloading PJM data")
    ap.add_argument("--pjm-dataset", default=PJM_DEFAULT_DATASET, help="PJM dataset name (default hrl_load_metered)")
    ap.add_argument("--pjm-api-key-env", default=PJM_DEFAULT_ENV, help="Env var containing PJM API key")
    ap.add_argument("--pjm-row-count", type=int, default=50000, help="Rows per PJM API request (default 50000)")
    ap.add_argument("--pjm-out", type=Path, default=None, help="Optional custom PJM CSV path")
    ap.add_argument("--weather-out", type=Path, default=None, help="Optional custom weather CSV path")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    if not args.skip_weather:
        weather_path = args.weather_out or weather_out_path(args.year, args.month)
        download_weather_month(args.zones_json, args.year, args.month, Path(weather_path))
    else:
        print("[WX] Skipped weather download")

    if not args.skip_pjm:
        download_pjm(args)
    else:
        print("[PJM] Skipped PJM download")


if __name__ == "__main__":
    main()
