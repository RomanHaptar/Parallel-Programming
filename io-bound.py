import os
import random
import time
import shutil
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        wrapper.last_time = end - start
        print(f"[{func.__name__}] Час: {wrapper.last_time:.4f} сек.")
        return result

    return wrapper


NUM_FILES = 1000


def generate_test_files(base_dir):
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    words = ["університет", "лабораторна", "студент", "код", "система", "потік"]
    for i in range(NUM_FILES):
        sub_dir = os.path.join(base_dir, f"folder_{i // 100}")
        os.makedirs(sub_dir, exist_ok=True)
        filepath = os.path.join(sub_dir, f"file_{i}.txt")
        content = " ".join(random.choices(words, k=random.randint(500, 2000)))
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)


def count_words_in_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return len(f.read().split())


def get_all_files_recursive(base_dir):
    file_paths = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".txt"):
                file_paths.append(os.path.join(root, file))
    return file_paths


@timer
def sequential_io(filepaths):
    return sum(count_words_in_file(path) for path in filepaths)


@timer
def parallel_io(filepaths, num_threads):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        return sum(executor.map(count_words_in_file, filepaths))


def run_test_isolation(threads=None):
    test_dir = f"test_io_data_{threads if threads else 'seq'}"
    generate_test_files(test_dir)
    all_files = get_all_files_recursive(test_dir)

    if threads is None:
        sequential_io(all_files)
        time_taken = sequential_io.last_time
    else:
        parallel_io(all_files, threads)
        time_taken = parallel_io.last_time

    shutil.rmtree(test_dir)
    return time_taken


if __name__ == '__main__':
    threads_list = [1, 2, 4, 8, 16]
    times_io = []

    print("Тест 1: Послідовно...")
    times_io.append(run_test_isolation(threads=None))

    for w in threads_list[1:]:
        print(f"Тест: Паралельно {w} потоків...")
        times_io.append(run_test_isolation(threads=w))

    # Побудова графіка
    plt.figure(figsize=(8, 5))
    plt.plot(threads_list, times_io, marker='o', color='red', label='Читання файлів')
    plt.title('I/O-bound задача')
    plt.xlabel('Кількість потоків')
    plt.ylabel('Час виконання (сек)')
    plt.xticks(threads_list)
    plt.grid(True)
    plt.legend()
    plt.savefig('io_chart.png')
    print("\nГрафік збережено у файл 'io_chart.png'")