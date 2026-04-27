import argparse
import ctypes
import json
import mmap
import os
import random
import socket
import statistics
import struct
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TASK2_ROOT = Path(__file__).resolve().parents[1]
CS_HELPER_DIR = TASK2_ROOT / "cs_helper"
RESULTS_DIR = PROJECT_ROOT / "results"
IPC_RUNTIME_DIR = TASK2_ROOT / "ipc_runtime"
CS_HELPER_DLL = CS_HELPER_DIR / "bin" / "Release" / "net8.0" / "CsHelper.dll"
CS_HELPER_EXE = CS_HELPER_DIR / "bin" / "Release" / "net8.0" / "CsHelper.exe"


WAIT_OBJECT_0 = 0
WAIT_TIMEOUT = 258

class WinNamedEvent:
    def __init__(self, name: str):
        if os.name != "nt":
            raise RuntimeError("Named events for mmap benchmark are supported only on Windows.")

        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._kernel32.CreateEventW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_bool, ctypes.c_wchar_p]
        self._kernel32.CreateEventW.restype = ctypes.c_void_p
        self._kernel32.SetEvent.argtypes = [ctypes.c_void_p]
        self._kernel32.SetEvent.restype = ctypes.c_bool
        self._kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        self._kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        self._kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        self._kernel32.CloseHandle.restype = ctypes.c_bool

        handle = self._kernel32.CreateEventW(None, False, False, name)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        self._handle = handle

    def set(self):
        if not self._kernel32.SetEvent(self._handle):
            raise ctypes.WinError(ctypes.get_last_error())

    def wait(self, timeout_ms: int) -> bool:
        result = self._kernel32.WaitForSingleObject(self._handle, timeout_ms)
        if result == WAIT_OBJECT_0:
            return True
        if result == WAIT_TIMEOUT:
            return False
        raise ctypes.WinError(ctypes.get_last_error())

    def close(self):
        if getattr(self, "_handle", None):
            self._kernel32.CloseHandle(self._handle)
            self._handle = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

def parse_args():
    parser = argparse.ArgumentParser(description="Lab 3 / Task 2 IPC benchmark")
    parser.add_argument("--method", choices=["tcp", "file", "mmap", "all"], default="all")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--map-name", default="Lab3IpcMap")
    parser.add_argument("--mmap-size", type=int, default=64)
    parser.add_argument("--idle-timeout-ms", type=int, default=30000)
    parser.add_argument("--results-csv", default=str(RESULTS_DIR / "task2_results.csv"))
    parser.add_argument("--runtime-dir", default=str(IPC_RUNTIME_DIR))
    return parser.parse_args()


def ensure_dirs(runtime_dir: Path, results_csv: Path):
    runtime_dir.mkdir(parents=True, exist_ok=True)
    results_csv.parent.mkdir(parents=True, exist_ok=True)


def build_helper_command(method: str, args, runtime_dir: Path):
    log_path = runtime_dir / f"cs_helper_{method}.log"

    common_args = [
        f"mode={method}",
        f"port={args.port}",
        f"workdir={str(runtime_dir)}",
        f"mapName={args.map_name}",
        f"mmapSize={args.mmap_size}",
        f"idleTimeoutMs={args.idle_timeout_ms}",
        f"log={str(log_path)}",
    ]

    if CS_HELPER_EXE.exists():
        return [str(CS_HELPER_EXE), *common_args]

    if CS_HELPER_DLL.exists():
        return ["dotnet", str(CS_HELPER_DLL), *common_args]

    return [
        "dotnet",
        "run",
        "--project",
        str(CS_HELPER_DIR / "CsHelper.csproj"),
        "--configuration",
        "Release",
        "--",
        *common_args,
    ]


def start_helper(method: str, args, runtime_dir: Path):
    cmd = build_helper_command(method, args, runtime_dir)
    process = subprocess.Popen(
        cmd,
        cwd=str(CS_HELPER_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        bufsize=1,
    )
    wait_ready(process, timeout_sec=20)
    return process


def wait_ready(process: subprocess.Popen, timeout_sec: float):
    start = time.perf_counter()
    collected = []

    while True:
        if process.poll() is not None:
            output = "".join(collected)
            raise RuntimeError(f"C# helper завершився раніше часу.\n{output}")

        line = process.stdout.readline()
        if line:
            collected.append(line)
            if line.startswith("READY:"):
                return

        if time.perf_counter() - start > timeout_sec:
            raise TimeoutError("Не вдалося дочекатися READY від C# helper.")


def stop_helper(process: subprocess.Popen, timeout_sec: float = 5.0):
    try:
        process.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


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
        "notes": notes,
        "mean_ms": statistics.fmean(values_ms) if values_ms else 0.0,
        "median_ms": statistics.median(values_ms) if values_ms else 0.0,
        "p95_ms": percentile(values_ms, 95),
        "max_ms": max(values_ms) if values_ms else 0.0,
        "errors": errors,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def append_result_csv(results_csv: Path, summary: dict):
    file_exists = results_csv.exists()
    with results_csv.open("a", encoding="utf-8", newline="") as f:
        if not file_exists:
            f.write("TimestampUtc,Method,Iterations,MeanMs,MedianMs,P95Ms,MaxMs,Errors,MainProcess,HelperProcess,Notes\n")
        f.write(
            f'"{summary["timestamp_utc"]}",' 
            f'"{summary["method"]}",' 
            f'{summary["iterations"]},' 
            f'{summary["mean_ms"]:.6f},' 
            f'{summary["median_ms"]:.6f},' 
            f'{summary["p95_ms"]:.6f},' 
            f'{summary["max_ms"]:.6f},' 
            f'{summary["errors"]},' 
            f'"Python",' 
            f'"C#",' 
            f'"{summary["notes"]}"\n'
        )


def atomic_write_json(path: Path, payload: dict):
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(temp_path, path)


def wait_for_response_json(path: Path, expected_seq: int, timeout_sec: float):
    start = time.perf_counter()
    while True:
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as f:
                    payload = json.load(f)
                if payload.get("seq") == expected_seq:
                    return payload
            except (json.JSONDecodeError, OSError):
                pass

        if time.perf_counter() - start > timeout_sec:
            raise TimeoutError(f"Timeout while waiting for response seq={expected_seq}")

        time.sleep(0.001)


def benchmark_tcp(args, runtime_dir: Path):
    helper = start_helper("tcp", args, runtime_dir)
    rng = random.Random(args.seed)
    latencies = []
    errors = 0

    try:
        with socket.create_connection(("127.0.0.1", args.port), timeout=5) as sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            reader = sock.makefile("r", encoding="utf-8", newline="\n")
            writer = sock.makefile("w", encoding="utf-8", newline="\n")

            for seq in range(args.iterations):
                value = rng.randint(1, 1_000_000)

                start_ns = time.perf_counter_ns()
                writer.write(json.dumps({"seq": seq, "value": value}) + "\n")
                writer.flush()
                line = reader.readline()
                elapsed_ns = time.perf_counter_ns() - start_ns

                if not line:
                    errors += 1
                    continue

                response = json.loads(line)
                if response.get("seq") != seq or response.get("value") != value:
                    errors += 1

                latencies.append(elapsed_ns)

            writer.write(json.dumps({"seq": args.iterations, "value": 0, "terminate": True}) + "\n")
            writer.flush()

        return summarize("tcp", latencies, errors, args.iterations, "round-trip benchmark")
    finally:
        stop_helper(helper)


def benchmark_file(args, runtime_dir: Path):
    helper = start_helper("file", args, runtime_dir)
    rng = random.Random(args.seed)
    latencies = []
    errors = 0

    request_path = runtime_dir / "request.json"
    response_path = runtime_dir / "response.json"

    if request_path.exists():
        request_path.unlink()
    if response_path.exists():
        response_path.unlink()

    try:
        for seq in range(args.iterations):
            value = rng.randint(1, 1_000_000)

            if response_path.exists():
                response_path.unlink()

            start_ns = time.perf_counter_ns()
            atomic_write_json(request_path, {"seq": seq, "value": value})
            response = wait_for_response_json(response_path, seq, timeout_sec=10)
            elapsed_ns = time.perf_counter_ns() - start_ns

            if response.get("seq") != seq or response.get("value") != value:
                errors += 1

            latencies.append(elapsed_ns)

        atomic_write_json(request_path, {"seq": args.iterations, "value": 0, "terminate": True})
        return summarize("file", latencies, errors, args.iterations, "round-trip benchmark")
    finally:
        stop_helper(helper)


def write_i32(shared, offset: int, value: int):
    shared.seek(offset)
    shared.write(struct.pack("<i", value))


def read_i32(shared, offset: int) -> int:
    shared.seek(offset)
    return struct.unpack("<i", shared.read(4))[0]


def benchmark_mmap(args, runtime_dir: Path):
    if os.name != "nt":
        raise RuntimeError("mmap benchmark у цій реалізації підтримується лише на Windows.")

    helper = start_helper("mmap", args, runtime_dir)
    rng = random.Random(args.seed)
    latencies = []
    errors = 0

    request_event_name = args.map_name + "_REQ"
    response_event_name = args.map_name + "_RESP"

    try:
        with mmap.mmap(-1, args.mmap_size, tagname=args.map_name) as shared, \
             WinNamedEvent(request_event_name) as request_event, \
             WinNamedEvent(response_event_name) as response_event:

            shared.seek(0)
            shared.write(b"\x00" * args.mmap_size)

            for seq in range(args.iterations):
                value = rng.randint(1, 1_000_000)

                write_i32(shared, 0, 0)      # terminate flag
                write_i32(shared, 4, seq)    # seq
                write_i32(shared, 8, value)  # request value
                write_i32(shared, 12, 0)     # response value

                start_ns = time.perf_counter_ns()
                request_event.set()

                if not response_event.wait(10_000):
                    errors += 1
                    continue

                elapsed_ns = time.perf_counter_ns() - start_ns
                response_value = read_i32(shared, 12)

                if response_value != value:
                    errors += 1

                latencies.append(elapsed_ns)

            write_i32(shared, 0, 1)
            write_i32(shared, 4, args.iterations)
            write_i32(shared, 8, 0)
            request_event.set()

        return summarize("mmap", latencies, errors, args.iterations, "round-trip benchmark (event-based shared memory)")
    finally:
        stop_helper(helper)


def print_summary(summary: dict):
    print(f"\n=== {summary['method'].upper()} ===")
    print(f"Iterations: {summary['iterations']}")
    print(f"Mean ms:   {summary['mean_ms']:.6f}")
    print(f"Median ms: {summary['median_ms']:.6f}")
    print(f"P95 ms:    {summary['p95_ms']:.6f}")
    print(f"Max ms:    {summary['max_ms']:.6f}")
    print(f"Errors:    {summary['errors']}")


def main():
    args = parse_args()

    runtime_dir = Path(args.runtime_dir).resolve()
    results_csv = Path(args.results_csv).resolve()

    ensure_dirs(runtime_dir, results_csv)

    methods = ["tcp", "file", "mmap"] if args.method == "all" else [args.method]

    for method in methods:
        if method == "tcp":
            summary = benchmark_tcp(args, runtime_dir)
        elif method == "file":
            summary = benchmark_file(args, runtime_dir)
        elif method == "mmap":
            summary = benchmark_mmap(args, runtime_dir)
        else:
            raise ValueError(f"Unknown method: {method}")

        append_result_csv(results_csv, summary)
        print_summary(summary)

    print(f"\nРезультати записані у: {results_csv}")


if __name__ == "__main__":
    main()
