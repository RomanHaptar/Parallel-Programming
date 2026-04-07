#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import matplotlib.pyplot as plt


TASK_SPECS = [
    {
        "csv_name": "number_stats_timings.csv",
        "title": "Number Stats",
        "output_name": "speedup_number_stats.png",
        "pattern_order": ["worker_pool", "fork_join", "map_reduce"],
        "label_map": {
            "worker_pool": "Worker Pool",
            "fork_join": "Fork-Join",
            "map_reduce": "Map-Reduce",
        },
        "size_col": None,
    },
    {
        "csv_name": "html_tag_timings.csv",
        "title": "HTML Tag Count",
        "output_name": "speedup_html_tag_count.png",
        "pattern_order": ["worker_pool", "fork_join", "map_reduce"],
        "label_map": {
            "worker_pool": "Worker Pool",
            "fork_join": "Fork-Join",
            "map_reduce": "Map-Reduce",
        },
        "size_col": "files_count",
    },
    {
        "csv_name": "matrix_timings.csv",
        "title": "Matrix Multiplication",
        "output_name": "speedup_matrix_multiplication.png",
        "pattern_order": ["worker_pool", "fork_join", "map_reduce"],
        "label_map": {
            "worker_pool": "Worker Pool",
            "fork_join": "Fork-Join",
            "map_reduce": "Map-Reduce",
        },
        "size_col": "size",
    },
    {
        "csv_name": "transaction_timings.csv",
        "title": "Transaction Processing",
        "output_name": "speedup_transaction_processing.png",
        "pattern_order": ["producer_consumer", "pipeline"],
        "label_map": {
            "producer_consumer": "Producer-Consumer",
            "pipeline": "Pipeline",
        },
        "size_col": "count",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build speedup charts from lab CSV files."
    )
    parser.add_argument(
        "--input-dir",
        default="results",
        help="Directory that contains the CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="charts",
        help="Directory where PNG charts and summary CSV files will be saved.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=160,
        help="Output image DPI.",
    )
    return parser.parse_args()


def load_latest_snapshot(csv_path: Path, size_col: Optional[str]) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"CSV is empty: {csv_path}")

    df["pattern"] = df["pattern"].astype(str).str.strip().str.lower()
    df["threads"] = pd.to_numeric(df["threads"], errors="raise")
    df["time_ms"] = pd.to_numeric(df["time_ms"], errors="raise")

    if size_col and size_col in df.columns:
        latest_size = df.iloc[-1][size_col]
        df = df[df[size_col] == latest_size].copy()

    # Keep the latest row for each (pattern, threads) pair.
    latest = (
        df.iloc[::-1]
        .drop_duplicates(subset=["pattern", "threads"], keep="first")
        .iloc[::-1]
        .copy()
    )

    latest = latest.sort_values(["threads", "pattern"]).reset_index(drop=True)
    return latest


def build_speedup_table(snapshot: pd.DataFrame, pattern_order: List[str]) -> pd.DataFrame:
    seq_rows = snapshot[(snapshot["pattern"] == "sequential") & (snapshot["threads"] == 1)]
    if seq_rows.empty:
        raise ValueError("Sequential baseline row not found.")

    sequential_time = float(seq_rows.iloc[-1]["time_ms"])

    rows: List[Dict[str, float]] = []
    thread_values = sorted(int(x) for x in snapshot["threads"].unique() if int(x) != 1)

    for threads in thread_values:
        row: Dict[str, float] = {"threads": threads}
        for pattern in pattern_order:
            match = snapshot[(snapshot["pattern"] == pattern) & (snapshot["threads"] == threads)]
            if match.empty:
                row[pattern] = float("nan")
            else:
                row[pattern] = sequential_time / float(match.iloc[-1]["time_ms"])
        rows.append(row)

    return pd.DataFrame(rows)


def save_speedup_table(speedup_df: pd.DataFrame, output_csv: Path) -> None:
    df = speedup_df.copy()
    for col in df.columns:
        if col != "threads":
            df[col] = df[col].round(4)
    df.to_csv(output_csv, index=False)


def plot_speedup_chart(
    speedup_df: pd.DataFrame,
    title: str,
    label_map: Dict[str, str],
    output_png: Path,
    dpi: int,
) -> None:
    plt.figure(figsize=(8, 5))

    x = speedup_df["threads"]
    for column in speedup_df.columns:
        if column == "threads":
            continue
        plt.plot(x, speedup_df[column], marker="o", label=label_map.get(column, column))

    plt.title(f"Speedup of {title}")
    plt.xlabel("Number of threads")
    plt.ylabel("Speedup = T_sequential / T_parallel")
    plt.xticks(list(x))
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_png, dpi=dpi)
    plt.close()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_files: List[Path] = []

    for spec in TASK_SPECS:
        csv_path = input_dir / spec["csv_name"]
        if not csv_path.exists():
            print(f"[skip] File not found: {csv_path}")
            continue

        snapshot = load_latest_snapshot(csv_path, spec["size_col"])
        speedup_df = build_speedup_table(snapshot, spec["pattern_order"])

        output_png = output_dir / spec["output_name"]
        plot_speedup_chart(
            speedup_df=speedup_df,
            title=spec["title"],
            label_map=spec["label_map"],
            output_png=output_png,
            dpi=args.dpi,
        )
        generated_files.append(output_png)

        output_csv = output_dir / output_png.with_suffix(".csv").name
        save_speedup_table(speedup_df, output_csv)
        generated_files.append(output_csv)

    print("Generated files:")
    for path in generated_files:
        print(path.resolve())


if __name__ == "__main__":
    main()
