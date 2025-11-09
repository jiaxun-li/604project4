# file: baseline_model.py
# Usage:
#   # Train from your existing PJM CSVs (incl. 2025 Jan–Oct and 2025 Nov)
#   python baseline_model.py train --pjm-glob "data/raw/pjm/hrl_load_metered_*.csv" --out models/baseline_hist_avg.parquet
#
#   # Predict next-day; auto-saves to predictions/MM-DD.csv
#   python baseline_model.py predict --date 2025-11-10 --model models/baseline_hist_avg.parquet
#
import argparse, glob, os
import pandas as pd
import numpy as np

BASELINE_PARQUET = "models/baseline_hist_avg.parquet"

def ensure_dirs():
    os.makedirs("models", exist_ok=True)
    os.makedirs("predictions", exist_ok=True)

def read_std_pjm(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [c.lower() for c in df.columns]
    # find timestamp
    ts = None
    for cand in ["timestamp","datetime","datetime_beginning_ept","datetime_beginning_utc","datetime_beginning_gmt","datetime_beginning"]:
        if cand in df.columns:
            ts = cand; break
    if ts is None:
        raise ValueError(f"No timestamp column in {csv_path}")
    # zone
    zone = None
    for cand in ["zone","zone_name","area","area_name"]:
        if cand in df.columns:
            zone = cand; break
    if zone is None:
        raise ValueError(f"No zone column in {csv_path}")
    # load
    load = None
    for cand in ["mw","load_mw","hrl_load","load","value"]:
        if cand in df.columns:
            load = cand; break
    if load is None:
        raise ValueError(f"No load column in {csv_path}")

    out = df[[ts, zone, load]].copy()
    out.columns = ["timestamp","zone","load_mw"]
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp","zone","load_mw"])
    out["zone"] = out["zone"].astype(str).str.strip().str.upper()
    return out

def load_pjm_glob(pattern: str) -> pd.DataFrame:
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No PJM files for pattern: {pattern}")
    frames = [read_std_pjm(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["zone","timestamp"]).sort_values(["zone","timestamp"])
    return df

def train_hist_avg(pjm_df: pd.DataFrame, out_path: str = BASELINE_PARQUET):
    ensure_dirs()
    df = pjm_df.copy()
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    grp = df.groupby(["zone","day_of_week","hour"], as_index=False)["load_mw"].mean()
    grp = grp.rename(columns={"load_mw":"load_mean"})
    grp.to_parquet(out_path, index=False)
    print(f"[MODEL] Wrote baseline means to {out_path} ({len(grp)} rows)")

def predict_day(date_str: str, model_path: str = BASELINE_PARQUET, zones=None, out_csv: str|None = None):
    ensure_dirs()
    target = pd.Timestamp(date_str).normalize()
    hours = pd.date_range(target, target + pd.Timedelta(hours=23), freq="H")
    means = pd.read_parquet(model_path)
    means["zone"] = means["zone"].astype(str)

    # fallbacks
    z_h = means.groupby(["zone","hour"], as_index=False)["load_mean"].mean().rename(columns={"load_mean":"fallback_zone_hour"})
    h = means.groupby(["hour"], as_index=False)["load_mean"].mean().rename(columns={"load_mean":"fallback_hour"})

    if zones is None:
        zones = sorted(means["zone"].unique().tolist())

    rows = []
    dow = target.dayofweek
    for z in zones:
        for ts in hours:
            hr = ts.hour
            m = means[(means["zone"]==z) & (means["day_of_week"]==dow) & (means["hour"]==hr)]
            if len(m):
                pred = float(m["load_mean"].values[0])
            else:
                m2 = z_h[(z_h["zone"]==z) & (z_h["hour"]==hr)]
                if len(m2):
                    pred = float(m2["fallback_zone_hour"].values[0])
                else:
                    pred = float(h[h["hour"]==hr]["fallback_hour"].values[0])
            rows.append({"date": target.date(), "zone": z, "timestamp": ts, "pred_load_mw": round(pred)})

    preds = pd.DataFrame(rows)
    if out_csv is None:
        out_csv = f"predictions/{target.strftime('%m-%d')}.csv"
    preds.to_csv(out_csv, index=False)
    print(f"[PREDICT] {len(preds)} rows → {out_csv}")
    return preds

def main():
    ap = argparse.ArgumentParser(description="Baseline model (train + predict)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_t = sub.add_parser("train", help="Train historical-average baseline from PJM CSVs")
    p_t.add_argument("--pjm-glob", required=True, help='Glob for PJM CSVs, e.g. "data/raw/pjm_*.csv"')
    p_t.add_argument("--out", default=BASELINE_PARQUET, help="Output model parquet")

    p_p = sub.add_parser("predict", help="Predict next-day using baseline model")
    p_p.add_argument("--date", required=True, help="Target date YYYY-MM-DD")
    p_p.add_argument("--model", default=BASELINE_PARQUET, help="Model parquet path")
    p_p.add_argument("--zones", default=None, help="Comma-separated zone list (default: all)")
    p_p.add_argument("--out", default=None, help="Optional custom output CSV (default: predictions/MM-DD.csv)")

    args = ap.parse_args()
    if args.cmd == "train":
        df = load_pjm_glob(args.pjm_glob)
        train_hist_avg(df, out_path=args.out)
    elif args.cmd == "predict":
        zones = None
        if args.zones:
            zones = [z.strip() for z in args.zones.split(",") if z.strip()]
        predict_day(args.date, model_path=args.model, zones=zones, out_csv=args.out)

if __name__ == "__main__":
    main()
