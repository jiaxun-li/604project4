# Usage examples:
#   python renew_weather_data.py --zones-json config/zone_coords.json              # (defaults: 2025-11)
#   python renew_weather_data.py --zones-json config/zone_coords.json --year 2025 --month 11
#
# Output:
#   data/raw/weather/weather_YYYY_mon.csv   e.g., data/raw/weather_2025_nov.csv

import argparse, os, json, time
import pandas as pd

def ensure_dir(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def month_bounds(year: int, month: int):
    start = pd.Timestamp(year=year, month=month, day=1)
    end   = (start + pd.offsets.MonthEnd(0))
    # Meteostat Hourly end is exclusive → add one day
    return start, end + pd.Timedelta(days=1)

def out_path_for(year: int, month: int):
    mon = pd.Timestamp(year=year, month=month, day=1).strftime("%b").lower()  # 'nov'
    return f"data/raw/weather/weather_{year}_{mon}.csv"

def download_weather_month(zones_json: str, year: int, month: int, out_csv: str):
    try:
        from meteostat import Point, Hourly
    except Exception as e:
        raise RuntimeError("Please install Meteostat first: `pip install meteostat`") from e

    with open(zones_json, "r") as f:
        zone_coords = json.load(f)

    ensure_dir(os.path.dirname(out_csv))
    start, end_excl = month_bounds(year, month)

    frames = []
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
            keep = ["timestamp", "zone"] + [c for c in ["temp","dwpt","rhum","prcp","wspd","pres","coco"] if c in df.columns]
            frames.append(df[keep])
            print(f"  ✓ {zone}: {len(df)} rows")
            time.sleep(0.15)
        except Exception as ex:
            print(f"  ! {zone}: {ex}")

    if not frames:
        raise RuntimeError("No weather retrieved.")
    out = pd.concat(frames, ignore_index=True).sort_values(["zone","timestamp"])
    out.to_csv(out_csv, index=False)
    print(f"[WX] Wrote {len(out):,} rows → {out_csv}")

def main():
    ap = argparse.ArgumentParser(description="Renew ONLY weather for a given month (default: Nov 2025).")
    ap.add_argument("--zones-json", required=True, help="Path to zone_coords.json")
    ap.add_argument("--year", type=int, default=2025, help="Year (default 2025)")
    ap.add_argument("--month", type=int, default=11, help="Month 1–12 (default 11)")
    args = ap.parse_args()

    out_csv = out_path_for(args.year, args.month)
    download_weather_month(args.zones_json, args.year, args.month, out_csv)

if __name__ == "__main__":
    main()
