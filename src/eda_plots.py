"""
Simple exploratory plots for PJM zone demand:
    1. Zone/date hourly profile with peak hour highlighted.
    2. Zone/month peak-load trajectory with the top 6 days labelled.
    3. Zone/month peak-hour trajectory (hour of daily max load).

Usage examples (run from src/):
    python eda_plots.py hourly AECO 2025-11-19 --outdir plots/
    python eda_plots.py peak-load AECO 2025 11
    python eda_plots.py peak-hour AECO 2025 11 --pjm-source processed
    python eda_plots.py weather AECO 2025-11-19 --features temp rhum wspd
    python eda_plots.py window AECO 2025-11-16

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
DEFAULT_WEATHER = BASE_DIR / "data" / "processed" / "weather_training.csv"
DEFAULT_PJM_RAW_DIR = BASE_DIR / "data" / "raw" / "pjm"
DEFAULT_OUTDIR = BASE_DIR / "plots"
DEFAULT_PJM_SOURCE = "raw"
DEFAULT_WEATHER_FEATURES = ["temp", "rhum", "dwpt", "wspd", "prcp", "pres"]


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


def load_weather(weather_path: Path) -> pd.DataFrame:
    df = pd.read_csv(weather_path)
    required = {"timestamp", "zone"}
    if not required.issubset(df.columns):
        raise ValueError(f"Weather CSV must include {required}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "zone"])
    df["zone"] = df["zone"].astype(str)
    df["date"] = df["timestamp"].dt.normalize()
    df["hour"] = df["timestamp"].dt.hour
    return df


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


def plot_hourly_window(df: pd.DataFrame, zone: str, start_date: str, outdir: Path, days: int = 4) -> Path:
    start = pd.Timestamp(start_date).normalize()
    if days <= 0:
        raise ValueError("days must be positive")
    end = start + pd.Timedelta(days=days)
    mask = (df["zone"].to_numpy() == zone) & (df["timestamp"].to_numpy() >= start) & (df["timestamp"].to_numpy() < end)
    zone_df = df.loc[mask].sort_values("timestamp")
    if zone_df.empty:
        raise RuntimeError(f"No data for zone {zone} between {start.date()} and {(end - pd.Timedelta(days=1)).date()}")
    zone_df = zone_df.copy()
    zone_df["date"] = zone_df["timestamp"].dt.normalize()

    fig, ax = plt.subplots(figsize=(10, 5))
    palette = plt.cm.get_cmap("tab10")
    max_load = None
    peak_info: tuple[pd.Timestamp, float] | None = None
    for idx, (day, day_df) in enumerate(zone_df.groupby("date", sort=True)):
        day_df = day_df.sort_values("timestamp")
        ax.plot(
            day_df["hour"],
            day_df["load"],
            marker="o",
            linewidth=1.3,
            color=palette(idx % palette.N),
            label=day.strftime("%Y-%m-%d"),
        )
        day_peak_idx = day_df["load"].idxmax()
        day_peak_load = day_df.loc[day_peak_idx, "load"]
        if max_load is None or day_peak_load > max_load:
            max_load = day_peak_load
            peak_info = (day_df.loc[day_peak_idx, "timestamp"], day_peak_load)

    if peak_info:
        ts, load = peak_info
        ax.scatter(
            [ts.hour],
            [load],
            color="red",
            zorder=5,
            label=f"Window peak ({ts.strftime('%m-%d %H:%M')})",
        )

    ax.set_xticks(range(0, 24, 2))
    ax.set_xlim(-0.5, 23.5)
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Load (MW)")
    ax.set_title(f"{zone} hourly load — {start.strftime('%Y-%m-%d')} +{days}d window")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", ncol=2)
    out_path = fig_path(outdir, f"eda_window_{zone}_{start.strftime('%Y-%m-%d')}_{days}d.png")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def _select_weather_columns(df: pd.DataFrame, requested: list[str] | None) -> list[str]:
    candidates = requested or DEFAULT_WEATHER_FEATURES
    cols = [col for col in candidates if col in df.columns]
    if cols:
        return cols
    numeric_cols = [
        col
        for col in df.columns
        if col not in {"timestamp", "zone", "date", "hour"} and pd.api.types.is_numeric_dtype(df[col])
    ]
    if not numeric_cols:
        raise RuntimeError("No numeric weather columns available to plot.")
    return numeric_cols


def plot_weather_features_with_load(
    pjm_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    zone: str,
    date: str,
    outdir: Path,
    weather_cols: list[str] | None = None,
) -> Path:
    if weather_df is None:
        raise ValueError("Weather data is required for weather plotting.")
    day = pd.Timestamp(date).normalize()
    load_df = _filter_zone_day(pjm_df, zone, day).sort_values("timestamp")
    if load_df.empty:
        raise RuntimeError(f"No PJM load data for zone {zone} on {day.date()}")
    zone_weather = _filter_zone_day(weather_df, zone, day).sort_values("timestamp")
    if zone_weather.empty:
        raise RuntimeError(f"No weather data for zone {zone} on {day.date()}")
    cols = _select_weather_columns(zone_weather, weather_cols)

    n_axes = 1 + len(cols)
    fig, axes = plt.subplots(n_axes, 1, figsize=(10, 2.5 * n_axes), sharex=True)
    if n_axes == 1:
        axes = [axes]

    load_ax = axes[0]
    load_ax.plot(load_df["timestamp"], load_df["load"], marker="o", color="tab:blue", label="Load (MW)")
    peak_idx = load_df["load"].idxmax()
    peak_row = load_df.loc[peak_idx]
    load_ax.scatter([peak_row["timestamp"]], [peak_row["load"]], color="red", zorder=5, label=f"Peak {int(peak_row['hour']):02d}h")
    load_ax.set_ylabel("Load (MW)")
    load_ax.set_title(f"{zone} load vs weather — {day.strftime('%Y-%m-%d')}")
    load_ax.grid(True, alpha=0.3)
    load_ax.legend(loc="upper left")

    for ax, col in zip(axes[1:], cols):
        ax.plot(zone_weather["timestamp"], zone_weather[col], color="tab:orange")
        ax.set_ylabel(col)
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Timestamp (EPT)")
    fig.autofmt_xdate()
    fig.tight_layout()
    out_path = fig_path(outdir, f"eda_weather_load_{zone}_{day.strftime('%Y-%m-%d')}.png")
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
    common.add_argument("--weather-csv", type=Path, default=DEFAULT_WEATHER, help="Weather CSV path (for weather plots)")
    common.add_argument("--outdir", type=Path, default=DEFAULT_OUTDIR, help="Output directory for figures")

    hourly = sub.add_parser("hourly", parents=[common], help="Plot hourly load profile for a zone/date")
    hourly.add_argument("zone", help="Zone code")
    hourly.add_argument("date", help="Date YYYY-MM-DD")

    window = sub.add_parser("window", parents=[common], help="Plot hourly load across a 4-day window")
    window.add_argument("zone", help="Zone code")
    window.add_argument("start_date", help="Start date YYYY-MM-DD")
    window.add_argument("--days", type=int, default=4, help="Number of consecutive days to plot (default 4)")

    peak_load = sub.add_parser("peak-load", parents=[common], help="Plot peak loads across a month")
    peak_load.add_argument("zone", help="Zone code")
    peak_load.add_argument("year", type=int, help="Calendar year")
    peak_load.add_argument("month", type=int, help="Calendar month (1-12)")

    peak_hour = sub.add_parser("peak-hour", parents=[common], help="Plot peak hours across a month")
    peak_hour.add_argument("zone", help="Zone code")
    peak_hour.add_argument("year", type=int, help="Calendar year")
    peak_hour.add_argument("month", type=int, help="Calendar month (1-12)")

    weather = sub.add_parser("weather", parents=[common], help="Plot hourly load and weather features for a zone/date")
    weather.add_argument("zone", help="Zone code")
    weather.add_argument("date", help="Date YYYY-MM-DD")
    weather.add_argument(
        "--features",
        nargs="+",
        default=None,
        help="Optional weather columns to plot (default: temp/rhum/dwpt/wspd/prcp/pres if available)",
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()
    df = load_pjm(args.pjm_source, args.pjm_csv, args.pjm_raw_dir)
    weather_df = None
    if args.command == "weather":
        weather_df = load_weather(args.weather_csv)

    if args.command == "hourly":
        out_path = plot_hourly_profile(df, args.zone, args.date, args.outdir)
    elif args.command == "window":
        out_path = plot_hourly_window(df, args.zone, args.start_date, args.outdir, args.days)
    elif args.command == "peak-load":
        out_path = plot_monthly_peak_loads(df, args.zone, args.year, args.month, args.outdir)
    elif args.command == "peak-hour":
        out_path = plot_monthly_peak_hours(df, args.zone, args.year, args.month, args.outdir)
    elif args.command == "weather":
        out_path = plot_weather_features_with_load(
            df,
            weather_df,
            args.zone,
            args.date,
            args.outdir,
            args.features,
        )
    else:
        raise ValueError(f"Unknown command {args.command}")

    print(str(out_path))


if __name__ == "__main__":
    main()
