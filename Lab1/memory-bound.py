import time
import multiprocessing as mp
import ctypes
import matplotlib.pyplot as plt

SIZE = 10000


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        wrapper.last_time = end - start
        print(f"[{func.__name__}] Час: {wrapper.last_time:.4f} сек.")
        return result

    return wrapper


def init_worker(src, dst):
    global shared_src, shared_dst
    shared_src = src
    shared_dst = dst


def transpose_worker(start_row, end_row):
    for i in range(start_row, end_row):
        for j in range(SIZE):
            shared_dst[j * SIZE + i] = shared_src[i * SIZE + j]


@timer
def parallel_shared_memory_transpose(workers, src_arr, dst_arr):
    chunk_size = SIZE // workers
    ranges = [(i * chunk_size, (i + 1) * chunk_size if i != workers - 1 else SIZE) for i in range(workers)]
    with mp.Pool(processes=workers, initializer=init_worker, initargs=(src_arr, dst_arr)) as pool:
        pool.starmap(transpose_worker, ranges)


if __name__ == '__main__':
    workers_list = [1, 2, 4, 8]
    times_memory = []

    print("Виділення спільної пам'яті...")
    src_matrix = mp.RawArray(ctypes.c_double, SIZE * SIZE)
    dst_matrix = mp.RawArray(ctypes.c_double, SIZE * SIZE)

    for w in workers_list:
        print(f"Паралельне транспонування ({w} процесів)...")
        parallel_shared_memory_transpose(w, src_matrix, dst_matrix)
        times_memory.append(parallel_shared_memory_transpose.last_time)

    # Побудова графіка
    plt.figure(figsize=(8, 5))
    plt.plot(workers_list, times_memory, marker='o', color='purple', label='Транспонування матриці')
    plt.title('Memory-bound задача')
    plt.xlabel('Кількість процесів')
    plt.ylabel('Час виконання (сек)')
    plt.xticks(workers_list)
    plt.grid(True)
    plt.legend()
    plt.savefig('memory_chart.png')
    print("\nГрафік збережено у файл 'memory_chart.png'")