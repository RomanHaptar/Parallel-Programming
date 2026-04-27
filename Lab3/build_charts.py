from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parent
RESULTS_DIR = PROJECT_ROOT / "results"
CHARTS_DIR = PROJECT_ROOT / "report_assets" / "charts"

TASK1_CSV = RESULTS_DIR / "task1_results.csv"
TASK2_ONE_ENV_CSV = RESULTS_DIR / "task2_one_env_results.csv"
TASK2_TWO_ENV_CSV = RESULTS_DIR / "task2_results.csv"


def ensure_dirs():
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)


def save_plot(fig, filename: str):
    output_path = CHARTS_DIR / filename
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {output_path}")


def build_task1_safe_vs_threads():
    if not TASK1_CSV.exists():
        print(f"Missing file: {TASK1_CSV}")
        return

    df = pd.read_csv(TASK1_CSV)

    safe_df = df[df["Mode"].str.lower() == "safe"].copy()
    if safe_df.empty:
        print("No 'safe' rows found in task1_results.csv")
        return

    safe_df = safe_df.sort_values("ThreadsCount")
    safe_df = safe_df.drop_duplicates(subset=["ThreadsCount"], keep="last")
    safe_df = safe_df.sort_values("ThreadsCount")

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(safe_df["ThreadsCount"], safe_df["ElapsedMs"], marker="o")
    ax.set_title("Залежність часу виконання safe-режиму від кількості потоків")
    ax.set_xlabel("Кількість потоків")
    ax.set_ylabel("Час виконання, мс")
    ax.grid(True)

    save_plot(fig, "task1_safe_vs_threads.png")


def build_task2_one_env_mean():
    if not TASK2_ONE_ENV_CSV.exists():
        print(f"Missing file: {TASK2_ONE_ENV_CSV}")
        return

    df = pd.read_csv(TASK2_ONE_ENV_CSV)

    if "Method" not in df.columns or "MeanMs" not in df.columns:
        print("task2_one_env_results.csv must contain columns: Method, MeanMs")
        return

    summary = (
        df.groupby("Method", as_index=False)["MeanMs"]
        .mean()
        .sort_values("MeanMs")
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(summary["Method"], summary["MeanMs"])
    ax.set_title("Порівняння середнього часу для методів у одному середовищі")
    ax.set_xlabel("Метод")
    ax.set_ylabel("Середній час, мс")
    ax.grid(True, axis="y")

    save_plot(fig, "task2_one_env_mean.png")


def build_task2_two_env_mean():
    if not TASK2_TWO_ENV_CSV.exists():
        print(f"Missing file: {TASK2_TWO_ENV_CSV}")
        return

    df = pd.read_csv(TASK2_TWO_ENV_CSV)

    if "Method" not in df.columns or "MeanMs" not in df.columns:
        print("task2_results.csv must contain columns: Method, MeanMs")
        return

    summary = (
        df.groupby("Method", as_index=False)["MeanMs"]
        .mean()
        .sort_values("MeanMs")
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(summary["Method"], summary["MeanMs"])
    ax.set_title("Порівняння середнього часу для методів у двох середовищах")
    ax.set_xlabel("Метод")
    ax.set_ylabel("Середній час, мс")
    ax.grid(True, axis="y")

    save_plot(fig, "task2_two_env_mean.png")


def main():
    ensure_dirs()
    build_task1_safe_vs_threads()
    build_task2_one_env_mean()
    build_task2_two_env_mean()


if __name__ == "__main__":
    main()