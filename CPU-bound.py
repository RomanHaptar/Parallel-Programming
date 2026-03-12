import random
import time
from concurrent.futures import ProcessPoolExecutor
import matplotlib.pyplot as plt

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        wrapper.last_time = end - start  # Збереження часу для графіка
        print(f"Час: {wrapper.last_time:.4f} сек.")
        return result

    return wrapper


# --- 1. ПІ ---
def monte_carlo_worker(iterations):
    inside_circle = 0
    for _ in range(iterations):
        x, y = random.random(), random.random()
        if x ** 2 + y ** 2 <= 1.0:
            inside_circle += 1
    return inside_circle


@timer
def sequential_pi(total_iterations):
    return (monte_carlo_worker(total_iterations) / total_iterations) * 4


@timer
def parallel_process_pi(total_iterations, num_workers):
    iterations_per_worker = total_iterations // num_workers
    inside_circle_total = 0
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(monte_carlo_worker, iterations_per_worker) for _ in range(num_workers)]
        for future in futures:
            inside_circle_total += future.result()
    return (inside_circle_total / total_iterations) * 4


# --- 2. ФАКТОРИЗАЦІЯ ---
def factorize_worker(n):
    factors = []
    d = 2
    while d * d <= n:
        while (n % d) == 0:
            factors.append(d)
            n //= d
        d += 1
    if n > 1:
        factors.append(n)
    return factors


@timer
def sequential_factorize(numbers):
    return [factorize_worker(n) for n in numbers]


@timer
def parallel_factorize(numbers, num_workers):
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        return list(executor.map(factorize_worker, numbers))


# --- 3. ПРОСТІ ЧИСЛА ---
def count_primes_worker(chunk):
    start, end = chunk
    count = 0
    for n in range(start, end):
        if n < 2: continue
        is_prime = True
        for i in range(2, int(n ** 0.5) + 1):
            if n % i == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return count


@timer
def parallel_primes(limit, num_workers):
    CHUNK_SIZE = 10000
    chunks = [(i, min(i + CHUNK_SIZE, limit)) for i in range(0, limit, CHUNK_SIZE)]
    total_primes = 0
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for count in executor.map(count_primes_worker, chunks):
            total_primes += count
    return total_primes


if __name__ == '__main__':
    WORKERS_LIST = [2, 4, 8]
    x_axis = [1] + WORKERS_LIST

    times_pi, times_fact, times_primes = [], [], []

    # Тест 1
    print("--- 1. Обчислення числа Пі (Монте-Карло) ---")
    print("Послідовно...")
    sequential_pi(20_000_000)
    times_pi.append(sequential_pi.last_time)
    for w in WORKERS_LIST:
        print(f"Паралельно ({w} процесів)...")
        parallel_process_pi(20_000_000, w)
        times_pi.append(parallel_process_pi.last_time)

    # Тест 2
    print("\n--- 2. Факторизація великих чисел ---")
    large_numbers = [random.randint(10 ** 13, 10 ** 14) for _ in range(40)]
    print("Послідовно...")
    sequential_factorize(large_numbers)
    times_fact.append(sequential_factorize.last_time)
    for w in WORKERS_LIST:
        print(f"Паралельно ({w} процесів)...")
        parallel_factorize(large_numbers, w)
        times_fact.append(parallel_factorize.last_time)

    # Тест 3
    print("\n--- 3. Обчислення простих чисел в діапазоні ---")
    LIMIT = 3_000_000
    print("Послідовно (1 процес)...")
    parallel_primes(LIMIT, 1)
    times_primes.append(parallel_primes.last_time)
    for w in WORKERS_LIST:
        print(f"Паралельно ({w} процесів)...")
        parallel_primes(LIMIT, w)
        times_primes.append(parallel_primes.last_time)

    # Побудова та збереження графіка
    plt.figure(figsize=(8, 5))
    plt.plot(x_axis, times_pi, marker='o', label='Монте-Карло (Пі)')
    plt.plot(x_axis, times_fact, marker='s', label='Факторизація')
    plt.plot(x_axis, times_primes, marker='^', label='Прості числа')
    plt.title('CPU-bound задачі')
    plt.xlabel('Кількість процесів')
    plt.ylabel('Час виконання (сек)')
    plt.xticks(x_axis)
    plt.grid(True)
    plt.legend()
    plt.savefig('cpu_chart.png')
    print("\nГрафік збережено у файл 'cpu_chart.png'")