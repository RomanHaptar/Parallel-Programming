import argparse
import json
import multiprocessing as mp
import os
import queue
import random
import statistics
import struct
import time
from multiprocessing import shared_memory
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TASK2_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "results"
RUNTIME_DIR = TASK2_ROOT / "ipc_runtime"


def parse_args():
    parser = argparse.ArgumentParser(description="Lab 3 / Task 2 / One environment benchmark")
    parser.add_argument("--method", choices=["pipe", "queue", "shared_memory", "all"], default="all")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--results-csv", default=str(RESULTS_DIR / "task2_one_env_results.csv"))
    parser.add_argument("--runtime-dir", default=str(RUNTIME_DIR / "one_env"))
    return parser.parse_args()


def ensure_dirs(runtime_dir: Path, results_csv: Path):
    runtime_dir.mkdir(parents=True, exist_ok=True)
    results_csv.parent.mkdir(parents=True, exist_ok=True)


def percentile(values, q):
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = max(0, min(len(sorted_values) - 1, int(round((q / 100) * (len(sorted_values) - 1)))))
    return sorted_values[index]


def summarize(method: str, latencies_ns, errors: int, iterations: int, notes: str):
    values_ms = [x / 1_000_000 for x in latencies_ns]
    return {
        "method": method,
        "iterations": iterations,
        "mean_ms": statistics.fmean(values_ms) if values_ms else 0.0,
        "median_ms": statistics.median(values_ms) if values_ms else 0.0,
        "p95_ms": percentile(values_ms, 95),
        "max_ms": max(values_ms) if values_ms else 0.0,
        "errors": errors,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "notes": notes,
    }


def append_result_csv(results_csv: Path, summary: dict):
    file_exists = results_csv.exists()
    with results_csv.open("a", encoding="utf-8", newline="") as f:
        if not file_exists:
            f.write("TimestampUtc,Method,Iterations,MeanMs,MedianMs,P95Ms,MaxMs,Errors,MainProcess,HelperProcess,Notes\n")
        f.write(
            f"\"{summary['timestamp_utc']}\","
            f"\"{summary['method']}\","
            f"{summary['iterations']},"
            f"{summary['mean_ms']:.6f},"
            f"{summary['median_ms']:.6f},"
            f"{summary['p95_ms']:.6f},"
            f"{summary['max_ms']:.6f},"
            f"{summary['errors']},"
            f"\"Python\","
            f"\"Python\","
            f"\"{summary['notes']}\"\n"
        )


def print_summary(summary: dict):
    print(f"\n=== {summary['method'].upper()} ===")
    print(f"Iterations: {summary['iterations']}")
    print(f"Mean ms:   {summary['mean_ms']:.6f}")
    print(f"Median ms: {summary['median_ms']:.6f}")
    print(f"P95 ms:    {summary['p95_ms']:.6f}")
    print(f"Max ms:    {summary['max_ms']:.6f}")
    print(f"Errors:    {summary['errors']}")


def append_log(log_path: Path, message: str):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")


# ---------------- PIPE ----------------

def pipe_worker(conn, log_path: str):
    log_file = Path(log_path)
    try:
        while True:
            request = conn.recv()
            if request.get("terminate"):
                append_log(log_file, "PIPE helper received terminate")
                break

            seq = request["seq"]
            value = request["value"]
            append_log(log_file, f"PIPE seq={seq} value={value}")

            conn.send({"seq": seq, "value": value, "logged": True, "status": "ok"})
    finally:
        conn.close()


def benchmark_pipe(args, runtime_dir: Path, ctx):
    parent_conn, child_conn = ctx.Pipe(duplex=True)
    log_path = runtime_dir / "pipe_worker.log"
    process = ctx.Process(target=pipe_worker, args=(child_conn, str(log_path)))
    process.start()
    child_conn.close()

    rng = random.Random(args.seed)
    latencies = []
    errors = 0

    try:
        for seq in range(args.iterations):
            value = rng.randint(1, 1_000_000)

            start_ns = time.perf_counter_ns()
            parent_conn.send({"seq": seq, "value": value})

            if not parent_conn.poll(10):
                errors += 1
                continue

            response = parent_conn.recv()
            elapsed_ns = time.perf_counter_ns() - start_ns

            if response.get("seq") != seq or response.get("value") != value:
                errors += 1

            latencies.append(elapsed_ns)

        parent_conn.send({"seq": args.iterations, "value": 0, "terminate": True})
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)

        return summarize("pipe", latencies, errors, args.iterations, "one environment / multiprocessing.Pipe")
    finally:
        parent_conn.close()


# ---------------- QUEUE ----------------

def queue_worker(request_queue, response_queue, log_path: str):
    log_file = Path(log_path)
    while True:
        try:
            request = request_queue.get(timeout=30)
        except queue.Empty:
            append_log(log_file, "QUEUE helper idle timeout")
            break

        if request.get("terminate"):
            append_log(log_file, "QUEUE helper received terminate")
            break

        seq = request["seq"]
        value = request["value"]
        append_log(log_file, f"QUEUE seq={seq} value={value}")

        response_queue.put({"seq": seq, "value": value, "logged": True, "status": "ok"})


def benchmark_queue(args, runtime_dir: Path, ctx):
    request_queue = ctx.Queue()
    response_queue = ctx.Queue()
    log_path = runtime_dir / "queue_worker.log"
    process = ctx.Process(target=queue_worker, args=(request_queue, response_queue, str(log_path)))
    process.start()

    rng = random.Random(args.seed)
    latencies = []
    errors = 0

    try:
        for seq in range(args.iterations):
            value = rng.randint(1, 1_000_000)

            start_ns = time.perf_counter_ns()
            request_queue.put({"seq": seq, "value": value})

            try:
                response = response_queue.get(timeout=10)
            except queue.Empty:
                errors += 1
                continue

            elapsed_ns = time.perf_counter_ns() - start_ns

            if response.get("seq") != seq or response.get("value") != value:
                errors += 1

            latencies.append(elapsed_ns)

        request_queue.put({"seq": args.iterations, "value": 0, "terminate": True})
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)

        return summarize("queue", latencies, errors, args.iterations, "one environment / multiprocessing.Queue")
    finally:
        request_queue.close()
        response_queue.close()


# ---------------- SHARED MEMORY ----------------

def shm_write_i32(shm_obj: shared_memory.SharedMemory, offset: int, value: int):
    shm_obj.buf[offset:offset + 4] = struct.pack("<i", value)


def shm_read_i32(shm_obj: shared_memory.SharedMemory, offset: int) -> int:
    return struct.unpack("<i", shm_obj.buf[offset:offset + 4])[0]


def shared_memory_worker(
    req_name: str,
    resp_name: str,
    request_event,
    response_event,
    terminate_event,
    log_path: str,
):
    log_file = Path(log_path)
    req_shm = shared_memory.SharedMemory(name=req_name)
    resp_shm = shared_memory.SharedMemory(name=resp_name)

    try:
        while True:
            if terminate_event.is_set():
                append_log(log_file, "SHM helper received terminate")
                break

            signaled = request_event.wait(timeout=30)
            if not signaled:
                append_log(log_file, "SHM helper idle timeout")
                break

            request_event.clear()

            if terminate_event.is_set():
                append_log(log_file, "SHM helper received terminate after signal")
                break

            seq = shm_read_i32(req_shm, 0)
            value = shm_read_i32(req_shm, 4)
            append_log(log_file, f"SHM seq={seq} value={value}")

            shm_write_i32(resp_shm, 0, seq)
            shm_write_i32(resp_shm, 4, value)
            response_event.set()
    finally:
        req_shm.close()
        resp_shm.close()


def benchmark_shared_memory(args, runtime_dir: Path, ctx):
    req_shm = shared_memory.SharedMemory(create=True, size=8)
    resp_shm = shared_memory.SharedMemory(create=True, size=8)
    request_event = ctx.Event()
    response_event = ctx.Event()
    terminate_event = ctx.Event()
    log_path = runtime_dir / "shared_memory_worker.log"

    process = ctx.Process(
        target=shared_memory_worker,
        args=(
            req_shm.name,
            resp_shm.name,
            request_event,
            response_event,
            terminate_event,
            str(log_path),
        ),
    )
    process.start()

    rng = random.Random(args.seed)
    latencies = []
    errors = 0

    try:
        for seq in range(args.iterations):
            value = rng.randint(1, 1_000_000)

            shm_write_i32(req_shm, 0, seq)
            shm_write_i32(req_shm, 4, value)
            response_event.clear()

            start_ns = time.perf_counter_ns()
            request_event.set()

            if not response_event.wait(timeout=10):
                errors += 1
                continue

            elapsed_ns = time.perf_counter_ns() - start_ns
            resp_seq = shm_read_i32(resp_shm, 0)
            resp_value = shm_read_i32(resp_shm, 4)

            if resp_seq != seq or resp_value != value:
                errors += 1

            latencies.append(elapsed_ns)

        terminate_event.set()
        request_event.set()

        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)

        return summarize(
            "shared_memory",
            latencies,
            errors,
            args.iterations,
            "one environment / multiprocessing.shared_memory",
        )
    finally:
        req_shm.close()
        resp_shm.close()
        req_shm.unlink()
        resp_shm.unlink()


def main():
    args = parse_args()

    runtime_dir = Path(args.runtime_dir).resolve()
    results_csv = Path(args.results_csv).resolve()

    ensure_dirs(runtime_dir, results_csv)

    ctx = mp.get_context("spawn")
    methods = ["pipe", "queue", "shared_memory"] if args.method == "all" else [args.method]

    summaries = []

    for method in methods:
        if method == "pipe":
            summary = benchmark_pipe(args, runtime_dir, ctx)
        elif method == "queue":
            summary = benchmark_queue(args, runtime_dir, ctx)
        elif method == "shared_memory":
            summary = benchmark_shared_memory(args, runtime_dir, ctx)
        else:
            raise ValueError(f"Unknown method: {method}")

        append_result_csv(results_csv, summary)
        print_summary(summary)
        summaries.append(summary)

    print(f"\nРезультати записані у: {results_csv}")


if __name__ == "__main__":
    mp.freeze_support()
    main()