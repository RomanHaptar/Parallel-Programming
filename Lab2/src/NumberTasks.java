import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

public final class NumberTasks {

    private NumberTasks() {
    }

    private static final class ChunkStats {
        final double min;
        final double max;
        final double sum;
        final int count;

        ChunkStats(double min, double max, double sum, int count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }
    }

    public static double[] generateRandomArray(int size, long seed, double minValue, double maxValue) {
        if (size <= 0) {
            throw new IllegalArgumentException("Розмір масиву має бути > 0");
        }
        if (minValue >= maxValue) {
            throw new IllegalArgumentException("minValue має бути < maxValue");
        }

        Random random = new Random(seed);
        double[] array = new double[size];
        double range = maxValue - minValue;

        for (int i = 0; i < size; i++) {
            array[i] = minValue + random.nextDouble() * range;
        }
        return array;
    }

    public static StatsResult sequential(double[] data) {
        validateInput(data);

        ChunkStats stats = computeChunkStats(data, 0, data.length);
        double median = medianSequential(data);
        double average = stats.sum / stats.count;

        return new StatsResult(stats.min, stats.max, average, median, data.length);
    }

    public static StatsResult workerPool(double[] data, int threads) throws Exception {
        validateInput(data);
        validateThreads(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<ChunkStats>> futures = new ArrayList<>();
            int chunkSize = (data.length + threads - 1) / threads;

            for (int start = 0; start < data.length; start += chunkSize) {
                int end = Math.min(start + chunkSize, data.length);
                int from = start;
                int to = end;

                Callable<ChunkStats> task = () -> computeChunkStats(data, from, to);
                futures.add(executor.submit(task));
            }

            ChunkStats total = null;
            for (Future<ChunkStats> future : futures) {
                ChunkStats partial = future.get();
                total = (total == null) ? partial : combine(total, partial);
            }

            double median = medianParallel(data);
            double average = total.sum / total.count;

            return new StatsResult(total.min, total.max, average, median, data.length);
        } finally {
            executor.shutdown();
        }
    }

    public static StatsResult forkJoin(double[] data, int parallelism) {
        validateInput(data);
        validateThreads(parallelism);

        int threshold = Math.max(10_000, data.length / (parallelism * 4));

        ForkJoinPool pool = new ForkJoinPool(parallelism);
        try {
            ChunkStats total = pool.invoke(new StatsRecursiveTask(data, 0, data.length, threshold));
            double median = medianParallel(data);
            double average = total.sum / total.count;

            return new StatsResult(total.min, total.max, average, median, data.length);
        } finally {
            pool.shutdown();
        }
    }

    public static StatsResult mapReduce(double[] data, int threads) throws Exception {
        validateInput(data);
        validateThreads(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            int chunkSize = Math.max(1, data.length / threads);
            List<Future<ChunkStats>> mapStage = new ArrayList<>();

            for (int start = 0; start < data.length; start += chunkSize) {
                int end = Math.min(start + chunkSize, data.length);
                int from = start;
                int to = end;

                mapStage.add(executor.submit(() -> computeChunkStats(data, from, to)));
            }

            List<ChunkStats> mappedResults = new ArrayList<>();
            for (Future<ChunkStats> future : mapStage) {
                mappedResults.add(future.get());
            }

            ChunkStats reduced = reduce(mappedResults);
            double median = medianParallel(data);
            double average = reduced.sum / reduced.count;

            return new StatsResult(reduced.min, reduced.max, average, median, data.length);
        } finally {
            executor.shutdown();
        }
    }

    private static ChunkStats reduce(List<ChunkStats> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("Немає що зменшувати у reduce");
        }

        ChunkStats total = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            total = combine(total, parts.get(i));
        }
        return total;
    }

    private static ChunkStats combine(ChunkStats a, ChunkStats b) {
        return new ChunkStats(
                Math.min(a.min, b.min),
                Math.max(a.max, b.max),
                a.sum + b.sum,
                a.count + b.count
        );
    }

    private static ChunkStats computeChunkStats(double[] data, int startInclusive, int endExclusive) {
        double min = data[startInclusive];
        double max = data[startInclusive];
        double sum = 0.0;

        for (int i = startInclusive; i < endExclusive; i++) {
            double value = data[i];
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
        }

        return new ChunkStats(min, max, sum, endExclusive - startInclusive);
    }

    private static double medianSequential(double[] data) {
        double[] copy = Arrays.copyOf(data, data.length);
        Arrays.sort(copy);
        return medianFromSorted(copy);
    }

    private static double medianParallel(double[] data) {
        double[] copy = Arrays.copyOf(data, data.length);
        Arrays.parallelSort(copy);
        return medianFromSorted(copy);
    }

    private static double medianFromSorted(double[] sorted) {
        int n = sorted.length;
        if (n % 2 == 0) {
            return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
        }
        return sorted[n / 2];
    }

    private static void validateInput(double[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Масив не може бути null або порожнім");
        }
    }

    private static void validateThreads(int threads) {
        if (threads <= 0) {
            throw new IllegalArgumentException("Кількість потоків має бути > 0");
        }
    }

    private static final class StatsRecursiveTask extends RecursiveTask<ChunkStats> {
        private final double[] data;
        private final int start;
        private final int end;
        private final int threshold;

        StatsRecursiveTask(double[] data, int start, int end, int threshold) {
            this.data = data;
            this.start = start;
            this.end = end;
            this.threshold = threshold;
        }

        @Override
        protected ChunkStats compute() {
            int length = end - start;
            if (length <= threshold) {
                return computeChunkStats(data, start, end);
            }

            int mid = start + length / 2;

            StatsRecursiveTask leftTask = new StatsRecursiveTask(data, start, mid, threshold);
            StatsRecursiveTask rightTask = new StatsRecursiveTask(data, mid, end, threshold);

            leftTask.fork();
            ChunkStats right = rightTask.compute();
            ChunkStats left = leftTask.join();

            return combine(left, right);
        }
    }
}
