"""
Weather data helper to download Meteostat hourly data for 2018–2025 and split it into per-year CSVs.

Examples:
    python weather_download.py download --zones-json config/zone_coords.json
    python weather_download.py split --in data/raw/noaa_hourly.csv --outdir data/raw/weather --overwrite
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd

DEFAULT_START = "2018-01-01"
DEFAULT_END = "2025-11-30"


def ensure_dir(path: str | Path) -> None:
    os.makedirs(path, exist_ok=True)


def download_weather(zones_json: Path, out_csv: Path, start: str, end: str) -> None:
    try:
        from meteostat import Point, Hourly
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Please `pip install meteostat` to use weather download.") from exc

    with zones_json.open("r") as handle:
        zone_coords = json.load(handle)

    ensure_dir(out_csv.parent if out_csv.parent != Path("") else ".")

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end) + pd.Timedelta(days=1)  # Meteostat end-exclusive

    frames: list[pd.DataFrame] = []
    print(f"[NOAA] Downloading {len(zone_coords)} zones from {start_ts.date()} to {end_ts.date() - pd.Timedelta(days=1)} …")
    for zone, coords in zone_coords.items():
        try:
            lat, lon = float(coords[0]), float(coords[1])
            loc = Point(lat, lon)
            data = Hourly(loc, start_ts, end_ts).fetch()
            if data is None or data.empty:
                print(f"  ! {zone}: no data")
                continue
            df = data.reset_index().rename(columns={"time": "timestamp"})
            df["zone"] = zone
            keep_cols = ["timestamp", "zone"] + [
                col for col in ["temp", "dwpt", "rhum", "prcp", "wspd", "pres", "coco"] if col in df.columns
            ]
            frames.append(df[keep_cols])
            print(f"  ✓ {zone}: {len(df)} rows")
            time.sleep(0.15)
        except Exception as err:  # pragma: no cover
            print(f"  ! {zone}: {err}")

    if not frames:
        raise RuntimeError("No weather retrieved; check coordinates/dates.")
    out_df = pd.concat(frames, ignore_index=True).sort_values(["zone", "timestamp"])
    out_df.to_csv(out_csv, index=False)
    print(f"[NOAA] Saved {len(out_df):,} rows to {out_csv}")


def split_weather(in_csv: Path, outdir: Path, overwrite: bool) -> None:
    """
    Split into 10 files:
      - weather_2018.csv … weather_2024.csv
      - weather_2025_jan_oct.csv
      - weather_2025_nov.csv
    Streamed in chunks to handle huge files.
    """
    ensure_dir(outdir)

    targets = {year: outdir / f"weather_{year}.csv" for year in range(2018, 2025)}
    targets_2025_jan_oct = outdir / "weather_2025_jan_oct.csv"
    targets_2025_nov = outdir / "weather_2025_nov.csv"

    all_paths = list(targets.values()) + [targets_2025_jan_oct, targets_2025_nov]

    if overwrite:
        for path in all_paths:
            if path.exists():
                path.unlink()

    header_written = {path: False for path in all_paths}

    chunk_iter = pd.read_csv(in_csv, chunksize=250_000)
    total = 0
    for chunk in chunk_iter:
        if "timestamp" not in chunk.columns:
            ts_col = next((c for c in chunk.columns if "time" in c.lower()), None)
            if ts_col is None:
                raise ValueError("Cannot find a timestamp column in weather CSV.")
            chunk = chunk.rename(columns={ts_col: "timestamp"})

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce")
        chunk = chunk.dropna(subset=["timestamp"])
        chunk["year"] = chunk["timestamp"].dt.year
        chunk["month"] = chunk["timestamp"].dt.month

        for year in range(2018, 2024 + 1):
            mask = chunk["year"].eq(year)
            if not mask.any():
                continue
            outpath = targets[year]
            mode = "a" if header_written[outpath] else "w"
            chunk.loc[mask].drop(columns=["year", "month"]).to_csv(
                outpath, index=False, mode=mode, header=not header_written[outpath]
            )
            header_written[outpath] = True

        mask_25_jan_oct = chunk["year"].eq(2025) & chunk["month"].between(1, 10)
        if mask_25_jan_oct.any():
            outpath = targets_2025_jan_oct
            mode = "a" if header_written[outpath] else "w"
            chunk.loc[mask_25_jan_oct].drop(columns=["year", "month"]).to_csv(
                outpath, index=False, mode=mode, header=not header_written[outpath]
            )
            header_written[outpath] = True

        mask_25_nov = chunk["year"].eq(2025) & chunk["month"].eq(11)
        if mask_25_nov.any():
            outpath = targets_2025_nov
            mode = "a" if header_written[outpath] else "w"
            chunk.loc[mask_25_nov].drop(columns=["year", "month"]).to_csv(
                outpath, index=False, mode=mode, header=not header_written[outpath]
            )
            header_written[outpath] = True

        mask_25_dec = chunk["year"].eq(2025) & chunk["month"].eq(12)
        if mask_25_dec.any():
            print("  ! Found records for 2025-12; not written (spec asks only Jan–Oct and Nov).")

        total += len(chunk)

    print(f"[SPLIT] Processed ~{total:,} rows from {in_csv}")
    print("[SPLIT] Wrote files:")
    for path, wrote in header_written.items():
        if wrote:
            print(f"  - {path}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Weather download & split helper")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_d = sub.add_parser("download", help="Download Meteostat hourly weather between 2018 and 2025")
    p_d.add_argument("--zones-json", type=Path, required=True, help="Path to zone_coords.json")
    p_d.add_argument("--out", type=Path, default=Path("data/raw/noaa_hourly.csv"), help="Output CSV path")
    p_d.add_argument("--start", default=DEFAULT_START, help="Start date YYYY-MM-DD (default 2018-01-01)")
    p_d.add_argument("--end", default=DEFAULT_END, help="End date YYYY-MM-DD (default 2025-11-30)")

    p_s = sub.add_parser("split", help="Split a weather CSV into yearly files")
    p_s.add_argument("--in", dest="in_csv", type=Path, required=True, help="Input weather CSV")
    p_s.add_argument("--outdir", type=Path, required=True, help="Output directory for split files")
    p_s.add_argument("--overwrite", action="store_true", help="Overwrite existing split files")

    return ap.parse_args()


def main() -> None:
    args = parse_args()
    if args.cmd == "download":
        download_weather(args.zones_json, args.out, args.start, args.end)
    elif args.cmd == "split":
        split_weather(args.in_csv, args.outdir, args.overwrite)


if __name__ == "__main__":
    main()
