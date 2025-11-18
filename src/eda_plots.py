"""
Simple exploratory plots for PJM zone demand:
    1. Zone/date hourly profile with peak hour highlighted.
    2. Zone/month peak-load trajectory with the top 6 days labelled.
    3. Zone/month peak-hour trajectory (hour of daily max load).

Usage examples (run from src/):
    python eda_plots.py hourly AECO 2025-11-19 --outdir plots/
    python eda_plots.py peak-load AECO 2025 11
    python eda_plots.py peak-hour AECO 2025 11 --pjm-source processed

By default the plots read raw PJM CSVs from data/raw/pjm so you can inspect the
latest downloads. Pass --pjm-source processed to use the cleaned training file
instead. Figures are saved to disk (default: plots/eda_<...>.png).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PJM = BASE_DIR / "data" / "processed" / "pjm_training.csv"
DEFAULT_PJM_RAW_DIR = BASE_DIR / "data" / "raw" / "pjm"
DEFAULT_OUTDIR = BASE_DIR / "plots"
DEFAULT_PJM_SOURCE = "raw"


def load_processed_pjm(pjm_path: Path) -> pd.DataFrame:
    df = pd.read_csv(pjm_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    df = df.dropna(subset=["timestamp", "zone", "load"])
    df["date"] = df["timestamp"].dt.normalize()
    df["hour"] = df["timestamp"].dt.hour
    return df


def load_raw_pjm(raw_dir: Path) -> pd.DataFrame:
    files = sorted(raw_dir.glob("hrl_load_metered_*.csv"))
    if not files:
        raise FileNotFoundError(f"No raw PJM CSVs found in {raw_dir}")
    frames = []
    for path in files:
        tmp = pd.read_csv(path)
        required = {"datetime_beginning_ept", "load_area", "mw"}
        if not required.issubset(tmp.columns):
            continue
        subset = tmp[["datetime_beginning_ept", "load_area", "mw"]].copy()
        subset.columns = ["timestamp", "zone", "load"]
        frames.append(subset)
    if not frames:
        raise RuntimeError(f"No compatible data found in {raw_dir}")
    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    df = df.dropna(subset=["timestamp", "zone", "load"])
    df["date"] = df["timestamp"].dt.normalize()
    df["hour"] = df["timestamp"].dt.hour
    return df


def load_pjm(pjm_source: str, processed_path: Path, raw_dir: Path) -> pd.DataFrame:
    if pjm_source == "processed":
        return load_processed_pjm(processed_path)
    if pjm_source == "raw":
        return load_raw_pjm(raw_dir)
    raise ValueError(f"Unknown PJM source {pjm_source}")


def fig_path(outdir: Path, name: str) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / name


def _filter_zone_day(df: pd.DataFrame, zone: str, day: pd.Timestamp) -> pd.DataFrame:
    mask = (df["zone"].to_numpy() == zone) & (df["date"].to_numpy() == day)
    return df.loc[mask]


def _filter_zone_month(df: pd.DataFrame, zone: str, year: int, month: int) -> pd.DataFrame:
    ts = df["timestamp"]
    mask = (
        (df["zone"].to_numpy() == zone)
        & (ts.dt.year.to_numpy() == year)
        & (ts.dt.month.to_numpy() == month)
    )
    return df.loc[mask]


def plot_hourly_profile(df: pd.DataFrame, zone: str, date: str, outdir: Path) -> Path:
    day = pd.Timestamp(date).normalize()
    zone_df = _filter_zone_day(df, zone, day).sort_values("timestamp")
    if zone_df.empty:
        raise RuntimeError(f"No data for zone {zone} on {day.date()}")
    peak_idx = zone_df["load"].idxmax()
    peak_row = zone_df.loc[peak_idx]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(zone_df["timestamp"], zone_df["load"], marker="o")
    ax.scatter([peak_row["timestamp"]], [peak_row["load"]], color="red", zorder=5, label=f"Peak hour {int(peak_row['hour']):02d}")
    ax.set_title(f"{zone} hourly load on {day.strftime('%Y-%m-%d')}")
    ax.set_xlabel("Timestamp (EPT)")
    ax.set_ylabel("Load (MW)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    out_path = fig_path(outdir, f"eda_hourly_{zone}_{day.strftime('%Y-%m-%d')}.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def compute_daily_peaks(zone_df: pd.DataFrame) -> pd.DataFrame:
    idx = zone_df.groupby("date")["load"].idxmax()
    daily = zone_df.loc[idx, ["date", "load", "hour"]].rename(columns={"load": "peak_load", "hour": "peak_hour"})
    daily["day_of_week"] = daily["date"].dt.day_name().str[:3]
    return daily


def plot_monthly_peak_loads(df: pd.DataFrame, zone: str, year: int, month: int, outdir: Path) -> Path:
    zone_df = _filter_zone_month(df, zone, year, month)
    if zone_df.empty:
        raise RuntimeError(f"No data for {zone} {year}-{month:02d}")
    daily = compute_daily_peaks(zone_df)
    daily = daily.sort_values("date")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(daily["date"], daily["peak_load"], marker="o")
    ax.set_title(f"{zone} daily peak load — {year}-{month:02d}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Peak load (MW)")
    ax.grid(True, alpha=0.3)

    top6 = daily.nlargest(6, "peak_load")
    for _, row in top6.iterrows():
        ax.scatter(row["date"], row["peak_load"], color="red", zorder=5)
        ax.annotate(
            f"{row['day_of_week']}",
            (row["date"], row["peak_load"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=8,
            color="red",
        )

    fig.autofmt_xdate()
    out_path = fig_path(outdir, f"eda_peak_loads_{zone}_{year}_{month:02d}.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_monthly_peak_hours(df: pd.DataFrame, zone: str, year: int, month: int, outdir: Path) -> Path:
    zone_df = _filter_zone_month(df, zone, year, month)
    if zone_df.empty:
        raise RuntimeError(f"No data for {zone} {year}-{month:02d}")
    daily = compute_daily_peaks(zone_df).sort_values("date")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.step(daily["date"], daily["peak_hour"], where="mid", label="Peak hour")
    ax.scatter(daily["date"], daily["peak_hour"], color="black", s=20)
    ax.set_title(f"{zone} daily peak hour — {year}-{month:02d}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Hour of day (0-23)")
    ax.set_ylim(-0.5, 23.5)
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    out_path = fig_path(outdir, f"eda_peak_hours_{zone}_{year}_{month:02d}.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "EDA plotting helpers for PJM zones.\n\n"
            "Examples:\n"
            "  python eda_plots.py hourly AECO 2025-11-19 --outdir plots/\n"
            "  python eda_plots.py peak-load AECO 2025 11\n"
            "  python eda_plots.py peak-hour AECO 2025 11 --pjm-csv data/processed/pjm_training.csv\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--pjm-source",
        choices=["processed", "raw"],
        default=DEFAULT_PJM_SOURCE,
        help=f"Select processed or raw inputs (default {DEFAULT_PJM_SOURCE})",
    )
    common.add_argument("--pjm-csv", type=Path, default=DEFAULT_PJM, help="Processed PJM CSV path")
    common.add_argument("--pjm-raw-dir", type=Path, default=DEFAULT_PJM_RAW_DIR, help="Directory containing raw PJM CSVs")
    common.add_argument("--outdir", type=Path, default=DEFAULT_OUTDIR, help="Output directory for figures")

    hourly = sub.add_parser("hourly", parents=[common], help="Plot hourly load profile for a zone/date")
    hourly.add_argument("zone", help="Zone code")
    hourly.add_argument("date", help="Date YYYY-MM-DD")

    peak_load = sub.add_parser("peak-load", parents=[common], help="Plot peak loads across a month")
    peak_load.add_argument("zone", help="Zone code")
    peak_load.add_argument("year", type=int, help="Calendar year")
    peak_load.add_argument("month", type=int, help="Calendar month (1-12)")

    peak_hour = sub.add_parser("peak-hour", parents=[common], help="Plot peak hours across a month")
    peak_hour.add_argument("zone", help="Zone code")
    peak_hour.add_argument("year", type=int, help="Calendar year")
    peak_hour.add_argument("month", type=int, help="Calendar month (1-12)")

    return ap.parse_args()


def main() -> None:
    args = parse_args()
    df = load_pjm(args.pjm_source, args.pjm_csv, args.pjm_raw_dir)

    if args.command == "hourly":
        out_path = plot_hourly_profile(df, args.zone, args.date, args.outdir)
    elif args.command == "peak-load":
        out_path = plot_monthly_peak_loads(df, args.zone, args.year, args.month, args.outdir)
    elif args.command == "peak-hour":
        out_path = plot_monthly_peak_hours(df, args.zone, args.year, args.month, args.outdir)
    else:
        raise ValueError(f"Unknown command {args.command}")

    print(str(out_path))


if __name__ == "__main__":
    main()
