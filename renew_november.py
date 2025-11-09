# file: renew_november.py
# Usage:
#   python renew_november.py --zones-json config/zone_coords.json
#
# Optional:
#   python renew_november.py --zones-json config/zone_coords.json --year 2025 --month 11
#
# Outputs:
#   data/raw/pjm_2025_nov.csv
#   data/raw/weather_split/weather_2025_nov.csv

import argparse, os, io, time, json
import pandas as pd
import requests
from datetime import datetime

# ------------------------
# Paths and helpers
# ------------------------
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def month_range(year: int, month: int):
    start = pd.Timestamp(year=year, month=month, day=1)
    end = (start + pd.offsets.MonthEnd(0)).normalize()
    return start, end

def output_names(year: int, month: int):
    mon_abbr = pd.Timestamp(year=year, month=month, day=1).strftime("%b").lower()
    pjm_path = f"data/raw/pjm_{year}_{mon_abbr}.csv"
    wx_path = f"data/raw/weather_{year}_{mon_abbr}.csv"
    return pjm_path, wx_path

# ------------------------
# PJM DataMiner2 feed API
# ------------------------
def download_pjm_feed(year: int, month: int, out_csv: str, limit: int = 50000):
    """
    Download PJM hourly load data via the JSON feed API:
    https://dataminer2.pjm.com/feed/hrl_load_metered
    """
    ensure_dir(os.path.dirname(out_csv))
    start_ts, end_ts = month_range(year, month)
    start_str = start_ts.strftime("%Y-%m-%dT00:00:00")
    end_str = (end_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

    url = "https://dataminer2.pjm.com/feed/hrl_load_metered"
    params = {
        "startRow": 1,
        "endRow": limit,
        "fields": "datetime_beginning_ept,zone,mw",
        "datetime_beginning_ept__gte": start_str,
        "datetime_beginning_ept__lt": end_str,
    }

    all_rows = []
    start_row = 1
    total = 0
    print(f"[PJM] Downloading from feed {start_str} → {end_str}")
    while True:
        params["startRow"] = start_row
        params["endRow"] = start_row + limit - 1
        try:
            resp = requests.get(url, params=params, timeout=90)
            resp.raise_for_status()
            js = resp.json()
            rows = js.get("data", [])
            if not rows:
                break
            df = pd.DataFrame(rows)
            df = df.rename(columns={
                "datetime_beginning_ept": "timestamp",
                "mw": "load_mw"
            })
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.dropna(subset=["timestamp", "zone", "load_mw"])
            df["zone"] = df["zone"].astype(str).str.strip().str.upper()
            all_rows.append(df)
            count = len(df)
            total += count
            print(f"  ✓ Rows {start_row:,}–{start_row+count-1:,} ({count:,})")
            if count < limit:
                break
            start_row += limit
            time.sleep(0.3)
        except Exception as e:
            print(f"  ⚠️ Error batch {start_row}-{start_row+limit-1}: {e}")
            time.sleep(2)
            break

    if not all_rows:
        raise RuntimeError("No PJM data downloaded.")
    df = pd.concat(all_rows, ignore_index=True).sort_values(["zone", "timestamp"])
    df.to_csv(out_csv, index=False)
    print(f"[PJM] Saved {len(df):,} rows → {out_csv}")

# ------------------------
# Weather (Meteostat / NOAA)
# ------------------------
def download_weather_month(zones_json: str, year: int, month: int, out_csv: str):
    try:
        from meteostat import Point, Hourly
    except ImportError:
        raise RuntimeError("Please install meteostat: pip install meteostat")

    ensure_dir(os.path.dirname(out_csv))
    with open(zones_json, "r") as f:
        zone_coords = json.load(f)

    start_ts, end_ts = month_range(year, month)
    s = start_ts
    e = end_ts + pd.Timedelta(days=1)
    frames = []

    print(f"[WX] Downloading weather for {len(zone_coords)} zones {s.date()} → {end_ts.date()}")
    for zone, coords in zone_coords.items():
        try:
            lat, lon = float(coords[0]), float(coords[1])
            loc = Point(lat, lon)
            data = Hourly(loc, s, e).fetch()
            if data.empty:
                print(f"  ! {zone}: no data")
                continue
            df = data.reset_index().rename(columns={"time": "timestamp"})
            df["zone"] = zone
            keep = ["timestamp", "zone"] + [c for c in ["temp", "dwpt", "rhum", "prcp", "wspd", "pres", "coco"] if c in df.columns]
            frames.append(df[keep])
            print(f"  ✓ {zone}: {len(df)} rows")
            time.sleep(0.2)
        except Exception as e:
            print(f"  ! {zone}: {e}")

    if not frames:
        raise RuntimeError("No weather retrieved for this month.")
    out = pd.concat(frames, ignore_index=True).sort_values(["zone", "timestamp"])
    out.to_csv(out_csv, index=False)
    print(f"[WX] Saved {len(out):,} rows → {out_csv}")

# ------------------------
# Main CLI
# ------------------------
def main():
    ap = argparse.ArgumentParser(description="Renew only one month of PJM + weather data (default: Nov 2025).")
    ap.add_argument("--zones-json", required=True, help="Path to zone_coords.json")
    ap.add_argument("--year", type=int, default=2025, help="Year to update (default 2025)")
    ap.add_argument("--month", type=int, default=11, help="Month to update (default 11)")
    args = ap.parse_args()

    pjm_path, wx_path = output_names(args.year, args.month)

    download_pjm_feed(args.year, args.month, pjm_path)
    download_weather_month(args.zones_json, args.year, args.month, wx_path)

if __name__ == "__main__":
    main()
