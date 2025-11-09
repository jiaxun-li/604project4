# file: weather_ops.py
# Usage:
#   # A) Download weather spanning your PJM data window
#   python weather_ops.py download --pjm-glob "data/raw/hrl_load_metered_*.csv" --zones-json config/zone_coords.json --out data/raw/noaa_hourly.csv
#
#   # B) Split a huge weather CSV into 11 files:
#   python weather_ops.py split --in data/raw/noaa_hourly.csv --outdir data/raw/weather --overwrite
#
# Output files:
#   data/raw/weather_split/
#     weather_2016.csv
#     ...
#     weather_2024.csv
#     weather_2025_jan_oct.csv
#     weather_2025_nov.csv

import argparse, os, json, glob, time
import pandas as pd

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def load_pjm_span(pjm_glob: str) -> tuple[str, str]:
    files = sorted(glob.glob(pjm_glob))
    if not files:
        raise FileNotFoundError(f"No PJM files matched pattern: {pjm_glob}")
    frames = []
    for f in files:
        df = pd.read_csv(f)
        df.columns = [c.lower() for c in df.columns]
        # detect time column
        ts_col = None
        for cand in ["timestamp","datetime","datetime_beginning_ept","datetime_beginning_utc","datetime_beginning_gmt","datetime_beginning"]:
            if cand in df.columns:
                ts_col = cand; break
        if ts_col is None:
            raise ValueError(f"Could not find timestamp column in {f}")
        t = pd.to_datetime(df[ts_col], errors="coerce").dropna()
        if t.empty:
            continue
        frames.append(pd.DataFrame({"timestamp":[t.min(), t.max()]}))
    if not frames:
        raise RuntimeError("No timestamps found across PJM files.")
    whole = pd.concat(frames, ignore_index=True)
    start = whole["timestamp"].min().normalize().strftime("%Y-%m-%d")
    end   = whole["timestamp"].max().normalize().strftime("%Y-%m-%d")
    return start, end

def download_weather(pjm_glob: str, zones_json: str, out_csv: str, start_override: str|None, end_override: str|None):
    try:
        from meteostat import Point, Hourly
    except Exception as e:
        raise RuntimeError("Please `pip install meteostat` to use weather download.") from e

    start, end = load_pjm_span(pjm_glob)
    if start_override: start = start_override
    if end_override:   end   = end_override

    with open(zones_json, "r") as f:
        zone_coords = json.load(f)
    ensure_dir(os.path.dirname(out_csv) or ".")

    s = pd.Timestamp(start)
    e = pd.Timestamp(end) + pd.Timedelta(days=1)  # meteostat end-exclusive
    frames = []
    print(f"[NOAA] Downloading {len(zone_coords)} zones from {start} to {end} …")
    for zone, coords in zone_coords.items():
        try:
            lat, lon = float(coords[0]), float(coords[1])
            loc = Point(lat, lon)
            data = Hourly(loc, s, e).fetch()
            if data is None or data.empty:
                print(f"  ! {zone}: no data")
                continue
            df = data.reset_index().rename(columns={"time":"timestamp"})
            df["zone"] = zone
            keep = ["timestamp","zone"] + [c for c in ["temp","dwpt","rhum","prcp","wspd","pres","coco"] if c in df.columns]
            frames.append(df[keep])
            print(f"  ✓ {zone}: {len(df)} rows")
            time.sleep(0.15)
        except Exception as ex:
            print(f"  ! {zone}: {ex}")

    if not frames:
        raise RuntimeError("No weather retrieved; check coords/dates.")
    out = pd.concat(frames, ignore_index=True).sort_values(["zone","timestamp"])
    out.to_csv(out_csv, index=False)
    print(f"[NOAA] Saved {len(out):,} rows to {out_csv}")

def split_weather(in_csv: str, outdir: str, overwrite: bool):
    """
    Split into 11 files:
      - weather_2016.csv … weather_2024.csv  (first 9 full years)
      - weather_2025_jan_oct.csv
      - weather_2025_nov.csv
    Streamed in chunks to handle huge files.
    """
    ensure_dir(outdir)

    # Prepare output file map
    targets = {year: os.path.join(outdir, f"weather_{year}.csv") for year in range(2016, 2025)}  # 2016..2024
    targets_2025_jan_oct = os.path.join(outdir, "weather_2025_jan_oct.csv")
    targets_2025_nov     = os.path.join(outdir, "weather_2025_nov.csv")

    all_paths = list(targets.values()) + [targets_2025_jan_oct, targets_2025_nov]

    # Clear previous files if overwrite
    if overwrite:
        for p in all_paths:
            if os.path.exists(p):
                os.remove(p)

    # Track whether headers were written
    header_written = {p: False for p in all_paths}

    # Iterate in chunks
    chunk_iter = pd.read_csv(in_csv, chunksize=250_000)
    total = 0
    for chunk in chunk_iter:
        # parse timestamp
        if "timestamp" not in chunk.columns:
            # try to auto-detect
            ts_col = next((c for c in chunk.columns if "time" in c.lower()), None)
            if ts_col is None:
                raise ValueError("Cannot find a timestamp column in weather CSV.")
            chunk = chunk.rename(columns={ts_col:"timestamp"})

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce")
        chunk = chunk.dropna(subset=["timestamp"])

        chunk["year"]  = chunk["timestamp"].dt.year
        chunk["month"] = chunk["timestamp"].dt.month

        # 2016..2024 straight to per-year files
        for year in range(2016, 2024+1):
            mask = chunk["year"].eq(year)
            if not mask.any():
                continue
            outpath = targets[year]
            mode = "a" if header_written[outpath] else "w"
            chunk.loc[mask].drop(columns=["year","month"]).to_csv(outpath, index=False, mode=mode, header=not header_written[outpath])
            header_written[outpath] = True

        # 2025 Jan–Oct
        mask_25_jan_oct = chunk["year"].eq(2025) & chunk["month"].between(1,10)
        if mask_25_jan_oct.any():
            outpath = targets_2025_jan_oct
            mode = "a" if header_written[outpath] else "w"
            chunk.loc[mask_25_jan_oct].drop(columns=["year","month"]).to_csv(outpath, index=False, mode=mode, header=not header_written[outpath])
            header_written[outpath] = True

        # 2025 Nov
        mask_25_nov = chunk["year"].eq(2025) & chunk["month"].eq(11)
        if mask_25_nov.any():
            outpath = targets_2025_nov
            mode = "a" if header_written[outpath] else "w"
            chunk.loc[mask_25_nov].drop(columns=["year","month"]).to_csv(outpath, index=False, mode=mode, header=not header_written[outpath])
            header_written[outpath] = True

        # (Optional) Warn if December 2025 appears (not requested)
        mask_25_dec = chunk["year"].eq(2025) & chunk["month"].eq(12)
        if mask_25_dec.any():
            print("  ! Found records for 2025-12; not written (spec asks only Jan–Oct and Nov).")

        total += len(chunk)

    print(f"[SPLIT] Processed ~{total:,} rows from {in_csv}")
    print("[SPLIT] Wrote files:")
    for p, wrote in header_written.items():
        if wrote:
            print("  -", p)

def main():
    ap = argparse.ArgumentParser(description="Weather ops: download & split")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_d = sub.add_parser("download", help="Download NOAA/Meteostat hourly weather")
    p_d.add_argument("--pjm-glob", required=True, help='Glob for existing PJM CSVs, e.g. "data/raw/pjm_*.csv"')
    p_d.add_argument("--zones-json", required=True, help="Path to zone_coords.json")
    p_d.add_argument("--out", default="data/raw/noaa_hourly.csv", help="Output weather CSV")
    p_d.add_argument("--start", default=None, help="Optional start YYYY-MM-DD")
    p_d.add_argument("--end", default=None, help="Optional end YYYY-MM-DD")

    p_s = sub.add_parser("split", help="Split a huge weather CSV into 11 files")
    p_s.add_argument("--in", dest="in_csv", required=True, help="Input weather CSV (e.g., data/raw/noaa_hourly.csv)")
    p_s.add_argument("--outdir", required=True, help="Output directory for split files")
    p_s.add_argument("--overwrite", action="store_true", help="Overwrite existing split files")

    args = ap.parse_args()

    if args.cmd == "download":
        download_weather(args.pjm_glob, args.zones_json, args.out, args.start, args.end)
    elif args.cmd == "split":
        split_weather(args.in_csv, args.outdir, args.overwrite)

if __name__ == "__main__":
    main()
