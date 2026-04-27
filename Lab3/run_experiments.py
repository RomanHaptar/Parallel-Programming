import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
TASK1_DIR = PROJECT_ROOT / "task1-bank-csharp"
TASK2_ROOT = PROJECT_ROOT / "task2-ipc-py-cs"
TASK2_PY_DIR = TASK2_ROOT / "python_main"
TASK2_ONE_ENV_DIR = TASK2_ROOT / "python_one_env"
TASK2_CS_DIR = TASK2_ROOT / "cs_helper"
RESULTS_DIR = PROJECT_ROOT / "results"


def parse_int_list(value: str) -> list[int]:
    items = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        items.append(int(part))
    return items


def run_command(cmd: list[str], cwd: Path) -> None:
    printable = " ".join(f'"{x}"' if " " in x else x for x in cmd)
    print(f"\n>>> {printable}")
    print(f"cwd: {cwd}")

    started = time.perf_counter()
    subprocess.run(cmd, cwd=str(cwd), check=True)
    elapsed = time.perf_counter() - started

    print(f"OK ({elapsed:.2f} s)")


def ensure_results_dir() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clean_task1_outputs() -> None:
    csv_path = RESULTS_DIR / "task1_results.csv"
    if csv_path.exists():
        csv_path.unlink()


def clean_task2_outputs() -> None:
    csv_cross = RESULTS_DIR / "task2_results.csv"
    if csv_cross.exists():
       csv_cross.unlink()

    csv_one_env = RESULTS_DIR / "task2_one_env_results.csv"
    if csv_one_env.exists():
        csv_one_env.unlink()

    runtime_dir = TASK2_ROOT / "ipc_runtime"
    if runtime_dir.exists():
        shutil.rmtree(runtime_dir, ignore_errors=True)


def run_task1(args) -> None:
    print("\n================ TASK 1 ================")

    csv_path = RESULTS_DIR / "task1_results.csv"

    if csv_path.exists():
        csv_path.unlink()

    run_command(["dotnet", "build", "-c", args.configuration], cwd=TASK1_DIR)

    run_command(
        [
            "dotnet", "run", "--configuration", args.configuration, "--",
            "mode=sequential",
            f"accounts={args.task1_accounts}",
            "threads=1",
            f"totalTransfers={args.task1_total_transfers}",
            f"csv={csv_path}",
            f"seed={args.seed}",
        ],
        cwd=TASK1_DIR,
    )

    for threads in args.task1_safe_threads:
        run_command(
            [
                "dotnet", "run", "--configuration", args.configuration, "--",
                "mode=safe",
                f"accounts={args.task1_accounts}",
                f"threads={threads}",
                f"totalTransfers={args.task1_total_transfers}",
                f"csv={csv_path}",
                f"seed={args.seed}",
            ],
            cwd=TASK1_DIR,
        )

    for threads in args.task1_unsafe_threads:
        for repeat in range(1, args.task1_unsafe_repeats + 1):
            run_command(
                [
                    "dotnet", "run", "--configuration", args.configuration, "--",
                    "mode=unsafe",
                    f"accounts={args.task1_accounts}",
                    f"threads={threads}",
                    f"totalTransfers={args.task1_unsafe_total_transfers}",
                    f"csv={csv_path}",
                    f"seed={args.seed + threads + repeat}",
                ],
                cwd=TASK1_DIR,
            )

    for _ in range(1, args.task1_deadlock_repeats + 1):
        run_command(
            [
                "dotnet", "run", "--configuration", args.configuration, "--",
                "mode=deadlock-demo",
                f"csv={csv_path}",
                f"deadlockTimeoutMs={args.task1_deadlock_timeout_ms}",
            ],
            cwd=TASK1_DIR,
        )

    print(f"\nTask 1 завершено. CSV: {csv_path}")


def run_task2(args) -> None:
    print("\n================ TASK 2 ================")

    csv_path = RESULTS_DIR / "task2_results.csv"

    if csv_path.exists():
        csv_path.unlink()

    run_command(["dotnet", "build", "-c", args.configuration], cwd=TASK2_CS_DIR)

    method_list = args.task2_methods
    base_runtime_dir = TASK2_ROOT / "ipc_runtime"
    base_runtime_dir.mkdir(parents=True, exist_ok=True)

    for repeat in range(1, args.task2_repeats + 1):
        for method_index, method in enumerate(method_list):
            run_runtime_dir = base_runtime_dir / f"{method}_run_{repeat}"
            run_runtime_dir.mkdir(parents=True, exist_ok=True)

            port = args.task2_port + (repeat - 1) * 10 + method_index
            map_name = f"{args.task2_map_name}_{method}_{repeat}"

            run_command(
                [
                    sys.executable,
                    "main.py",
                    "--method", method,
                    "--iterations", str(args.task2_iterations),
                    "--seed", str(args.seed + repeat),
                    "--port", str(port),
                    "--map-name", map_name,
                    "--mmap-size", str(args.task2_mmap_size),
                    "--idle-timeout-ms", str(args.task2_idle_timeout_ms),
                    "--results-csv", str(csv_path),
                    "--runtime-dir", str(run_runtime_dir),
                ],
                cwd=TASK2_PY_DIR,
            )

    print(f"\nTask 2 завершено. CSV: {csv_path}")


def run_task2_one_env(args) -> None:
    print("\n================ TASK 2 / ONE ENV ================")

    csv_path = RESULTS_DIR / "task2_one_env_results.csv"
    base_runtime_dir = TASK2_ROOT / "ipc_runtime" / "one_env"
    base_runtime_dir.mkdir(parents=True, exist_ok=True)

    if csv_path.exists():
        csv_path.unlink()

    for repeat in range(1, args.task2_repeats + 1):
        for method in args.task2_one_env_methods:
            run_runtime_dir = base_runtime_dir / f"{method}_run_{repeat}"
            run_runtime_dir.mkdir(parents=True, exist_ok=True)

            run_command(
                [
                    sys.executable,
                    "one_env.py",
                    "--method", method,
                    "--iterations", str(args.task2_iterations),
                    "--seed", str(args.seed + repeat),
                    "--results-csv", str(csv_path),
                    "--runtime-dir", str(run_runtime_dir),
                ],
                cwd=TASK2_ONE_ENV_DIR,
            )

    print(f"\nTask 2 / one environment завершено. CSV: {csv_path}")


def build_parser():
    parser = argparse.ArgumentParser(description="Автоматичний запуск експериментів для лабораторної роботи №3")

    parser.add_argument("--task", choices=["task1", "task2", "all"], default="all", help="Що запускати")
    parser.add_argument("--configuration", default="Release", help="Конфігурація dotnet build/run")
    parser.add_argument("--seed", type=int, default=42, help="Базовий seed")
    parser.add_argument("--clean", action="store_true", help="Перед запуском очистити старі CSV та runtime-файли")

    parser.add_argument("--task1-accounts", type=int, default=200)
    parser.add_argument("--task1-total-transfers", type=int, default=1_000_000)
    parser.add_argument("--task1-safe-threads", type=parse_int_list, default=[1, 2, 4, 8, 16])
    parser.add_argument("--task1-unsafe-threads", type=parse_int_list, default=[1000, 2000])
    parser.add_argument("--task1-unsafe-total-transfers", type=int, default=200_000)
    parser.add_argument("--task1-unsafe-repeats", type=int, default=3)
    parser.add_argument("--task1-deadlock-repeats", type=int, default=1)
    parser.add_argument("--task1-deadlock-timeout-ms", type=int, default=2000)

    parser.add_argument("--task2-iterations", type=int, default=1000)
    parser.add_argument("--task2-repeats", type=int, default=3)
    parser.add_argument("--task2-one-env-methods-str", default="pipe,queue,shared_memory")
    parser.add_argument("--task2-methods-str", default="tcp,file,mmap")
    parser.add_argument("--task2-port", type=int, default=5001)
    parser.add_argument("--task2-map-name", default="Lab3IpcMap")
    parser.add_argument("--task2-mmap-size", type=int, default=64)
    parser.add_argument("--task2-idle-timeout-ms", type=int, default=30000)

    return parser


def normalize_args(args):
    args.task2_one_env_methods = [
        x.strip() for x in args.task2_one_env_methods_str.split(",")
        if x.strip()
    ]

    allowed_one_env = {"pipe", "queue", "shared_memory"}
    invalid_one_env = [m for m in args.task2_one_env_methods if m not in allowed_one_env]
    if invalid_one_env:
        raise ValueError(f"Невідомі task2 one-env методи: {invalid_one_env}")

    args.task2_methods = [
        x.strip() for x in args.task2_methods_str.split(",")
        if x.strip()
    ]

    allowed = {"tcp", "file", "mmap"}
    invalid = [m for m in args.task2_methods if m not in allowed]
    if invalid:
        raise ValueError(f"Невідомі task2 методи: {invalid}")

    if args.task1_accounts < 2:
        raise ValueError("task1-accounts має бути >= 2")
    if args.task1_total_transfers < 1:
        raise ValueError("task1-total-transfers має бути >= 1")
    if args.task1_unsafe_total_transfers < 1:
        raise ValueError("task1-unsafe-total-transfers має бути >= 1")
    if args.task2_iterations < 1:
        raise ValueError("task2-iterations має бути >= 1")
    if args.task2_repeats < 1:
        raise ValueError("task2-repeats має бути >= 1")


def main():
    parser = build_parser()
    args = parser.parse_args()
    normalize_args(args)

    ensure_results_dir()

    if args.clean:
        if args.task in ("task1", "all"):
            clean_task1_outputs()
        if args.task in ("task2", "all"):
            clean_task2_outputs()

    if args.task in ("task1", "all"):
        run_task1(args)
    if args.task in ("task2", "all"):
        run_task2_one_env(args)
        run_task2(args)

    print("\n================ DONE ================")
    print(f"Task 1 CSV: {RESULTS_DIR / 'task1_results.csv'}")
    print(f"Task 2 one-env CSV: {RESULTS_DIR / 'task2_one_env_results.csv'}")
    print(f"Task 2 cross-language CSV: {RESULTS_DIR / 'task2_results.csv'}")


if __name__ == "__main__":
    main()
