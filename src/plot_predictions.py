"""
Plot predicted vs. actual hourly load for a specific zone/day using the CSV outputs
from xgboost_hourly_load.py (predicted_load + actual_load columns).

Usage:
    python plot_predictions.py predictions/xgboost_2024-12-03.csv AECO 2024-12-03 --outdir plots/
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTDIR = BASE_DIR / "plots"


def load_predictions(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = {"timestamp", "zone", "predicted_load"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{csv_path} missing required columns: {', '.join(sorted(missing))}")
    if "actual_load" not in df.columns:
        raise ValueError(f"{csv_path} does not include 'actual_load' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "zone"])
    df["zone"] = df["zone"].astype(str)
    df["date"] = df["timestamp"].dt.normalize()
    return df


def plot_prediction_day(df: pd.DataFrame, zone: str, date: str, outdir: Path) -> Path:
    day = pd.Timestamp(date).normalize()
    mask = (df["zone"].to_numpy() == zone) & (df["date"].to_numpy() == day)
    subset = df.loc[mask].sort_values("timestamp").copy()
    if subset.empty:
        raise RuntimeError(f"No rows for zone {zone} on {day.date()}")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(subset["timestamp"], subset["predicted_load"], label="Predicted", color="tab:orange", linewidth=2)
    if subset["actual_load"].notna().any():
        ax.plot(subset["timestamp"], subset["actual_load"], label="Actual", color="tab:blue", linewidth=2, alpha=0.85)
    else:
        ax.text(
            0.02,
            0.92,
            "Actual load unavailable",
            transform=ax.transAxes,
            fontsize=10,
            color="gray",
        )

    ax.set_title(f"{zone} hourly load prediction — {day.strftime('%Y-%m-%d')}")
    ax.set_xlabel("Timestamp (EPT)")
    ax.set_ylabel("Load (MW)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"pred_vs_actual_{zone}_{day.strftime('%Y-%m-%d')}.png"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Plot predicted vs. actual hourly load for a zone/day.")
    ap.add_argument("predictions_csv", type=Path, help="CSV produced by xgboost_hourly_load.py")
    ap.add_argument("zone", help="Zone code")
    ap.add_argument("date", help="Target date YYYY-MM-DD")
    ap.add_argument("--outdir", type=Path, default=DEFAULT_OUTDIR, help="Directory to save the figure")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    df = load_predictions(args.predictions_csv)
    out_path = plot_prediction_day(df, args.zone, args.date, args.outdir)
    print(out_path)


if __name__ == "__main__":
    main()
