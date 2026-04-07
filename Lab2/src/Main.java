import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        try {
            runNumberStatsExperiment();
            System.out.println();

            runHtmlExperiment();
            System.out.println();

            runMatrixExperiment();
            System.out.println();

            runTransactionExperiment();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runNumberStatsExperiment() throws Exception {
        int size = 1_000_000;
        long seed = 42L;

        double[] data = NumberTasks.generateRandomArray(size, seed, -1_000_000.0, 1_000_000.0);

        Path csvPath = Path.of("results", "number_stats_timings.csv");
        BenchmarkUtils.ensureCsvWithHeader(
                csvPath,
                "pattern,threads,time_ms,min,max,average,median,size"
        );

        System.out.println("=== NUMBER STATS EXPERIMENT ===");
        System.out.println("Array size: " + size);
        System.out.println();

        BenchmarkUtils.TimedValue<StatsResult> sequential = BenchmarkUtils.measure(
                "Sequential",
                () -> NumberTasks.sequential(data)
        );
        BenchmarkUtils.printTimedStats(sequential);
        saveStatsCsv(csvPath, "sequential", 1, sequential);

        List<Integer> threadOptions = buildThreadOptions();

        for (int threads : threadOptions) {
            BenchmarkUtils.TimedValue<StatsResult> workerPool = BenchmarkUtils.measure(
                    "WorkerPool-" + threads,
                    () -> NumberTasks.workerPool(data, threads)
            );
            BenchmarkUtils.printTimedStats(workerPool);
            saveStatsCsv(csvPath, "worker_pool", threads, workerPool);

            BenchmarkUtils.TimedValue<StatsResult> forkJoin = BenchmarkUtils.measure(
                    "ForkJoin-" + threads,
                    () -> NumberTasks.forkJoin(data, threads)
            );
            BenchmarkUtils.printTimedStats(forkJoin);
            saveStatsCsv(csvPath, "fork_join", threads, forkJoin);

            BenchmarkUtils.TimedValue<StatsResult> mapReduce = BenchmarkUtils.measure(
                    "MapReduce-" + threads,
                    () -> NumberTasks.mapReduce(data, threads)
            );
            BenchmarkUtils.printTimedStats(mapReduce);
            saveStatsCsv(csvPath, "map_reduce", threads, mapReduce);

            System.out.println();
        }

        System.out.println("Готово. Результати збережено у: " + csvPath.toAbsolutePath());
    }

    private static void runHtmlExperiment() throws Exception {
        Path htmlDir = Path.of("data", "html");
        List<Path> htmlFiles = HtmlTasks.loadHtmlFiles(htmlDir);

        if (htmlFiles.size() < 1000) {
            System.out.println("Увага: знайдено менше 1000 HTML-файлів: " + htmlFiles.size());
            System.out.println("Для вимог лаби бажано підготувати щонайменше 1000 файлів.");
            System.out.println();
        }

        Path csvPath = Path.of("results", "html_tag_timings.csv");
        BenchmarkUtils.ensureCsvWithHeader(
                csvPath,
                "pattern,threads,time_ms,files_count,unique_tags,top5"
        );

        System.out.println("=== HTML TAG COUNT EXPERIMENT ===");
        System.out.println("Files count: " + htmlFiles.size());
        System.out.println("Dataset dir: " + htmlDir.toAbsolutePath());
        System.out.println();

        BenchmarkUtils.TimedValue<Map<String, Integer>> sequential = BenchmarkUtils.measure(
                "HTML-Sequential",
                () -> HtmlTasks.sequential(htmlFiles)
        );
        printHtmlResult(sequential);
        saveHtmlCsv(csvPath, "sequential", 1, htmlFiles.size(), sequential);

        List<Integer> threadOptions = buildThreadOptions();

        for (int threads : threadOptions) {
            BenchmarkUtils.TimedValue<Map<String, Integer>> workerPool = BenchmarkUtils.measure(
                    "HTML-WorkerPool-" + threads,
                    () -> HtmlTasks.workerPool(htmlFiles, threads)
            );
            printHtmlResult(workerPool);
            saveHtmlCsv(csvPath, "worker_pool", threads, htmlFiles.size(), workerPool);

            BenchmarkUtils.TimedValue<Map<String, Integer>> forkJoin = BenchmarkUtils.measure(
                    "HTML-ForkJoin-" + threads,
                    () -> HtmlTasks.forkJoin(htmlFiles, threads)
            );
            printHtmlResult(forkJoin);
            saveHtmlCsv(csvPath, "fork_join", threads, htmlFiles.size(), forkJoin);

            BenchmarkUtils.TimedValue<Map<String, Integer>> mapReduce = BenchmarkUtils.measure(
                    "HTML-MapReduce-" + threads,
                    () -> HtmlTasks.mapReduce(htmlFiles, threads)
            );
            printHtmlResult(mapReduce);
            saveHtmlCsv(csvPath, "map_reduce", threads, htmlFiles.size(), mapReduce);

            System.out.println();
        }

        System.out.println("Готово. Результати збережено у: " + csvPath.toAbsolutePath());
    }

    private static void runMatrixExperiment() throws Exception {
        int size = 400;
        long seedA = 777L;
        long seedB = 888L;

        double[][] a = MatrixTasks.generateRandomMatrix(size, size, seedA, -10.0, 10.0);
        double[][] b = MatrixTasks.generateRandomMatrix(size, size, seedB, -10.0, 10.0);

        Path csvPath = Path.of("results", "matrix_timings.csv");
        BenchmarkUtils.ensureCsvWithHeader(
                csvPath,
                "pattern,threads,time_ms,size,checksum"
        );

        System.out.println("=== MATRIX MULTIPLICATION EXPERIMENT ===");
        System.out.println("Matrix size: " + size + "x" + size);
        System.out.println();

        BenchmarkUtils.TimedValue<double[][]> sequential = BenchmarkUtils.measure(
                "Matrix-Sequential",
                () -> MatrixTasks.sequential(a, b)
        );
        double sequentialChecksum = MatrixTasks.checksum(sequential.getValue());
        printMatrixResult(sequential, sequentialChecksum);
        saveMatrixCsv(csvPath, "sequential", 1, size, sequential, sequentialChecksum);

        List<Integer> threadOptions = buildThreadOptions();

        for (int threads : threadOptions) {
            BenchmarkUtils.TimedValue<double[][]> workerPool = BenchmarkUtils.measure(
                    "Matrix-WorkerPool-" + threads,
                    () -> MatrixTasks.workerPool(a, b, threads)
            );
            double workerChecksum = MatrixTasks.checksum(workerPool.getValue());
            assertClose(sequentialChecksum, workerChecksum, "worker_pool", threads);
            printMatrixResult(workerPool, workerChecksum);
            saveMatrixCsv(csvPath, "worker_pool", threads, size, workerPool, workerChecksum);

            BenchmarkUtils.TimedValue<double[][]> forkJoin = BenchmarkUtils.measure(
                    "Matrix-ForkJoin-" + threads,
                    () -> MatrixTasks.forkJoin(a, b, threads)
            );
            double forkJoinChecksum = MatrixTasks.checksum(forkJoin.getValue());
            assertClose(sequentialChecksum, forkJoinChecksum, "fork_join", threads);
            printMatrixResult(forkJoin, forkJoinChecksum);
            saveMatrixCsv(csvPath, "fork_join", threads, size, forkJoin, forkJoinChecksum);

            BenchmarkUtils.TimedValue<double[][]> mapReduce = BenchmarkUtils.measure(
                    "Matrix-MapReduce-" + threads,
                    () -> MatrixTasks.mapReduce(a, b, threads)
            );
            double mapReduceChecksum = MatrixTasks.checksum(mapReduce.getValue());
            assertClose(sequentialChecksum, mapReduceChecksum, "map_reduce", threads);
            printMatrixResult(mapReduce, mapReduceChecksum);
            saveMatrixCsv(csvPath, "map_reduce", threads, size, mapReduce, mapReduceChecksum);

            System.out.println();
        }

        System.out.println("Готово. Результати збережено у: " + csvPath.toAbsolutePath());
    }

    private static void runTransactionExperiment() throws Exception {
        int transactionCount = 2_000_000;
        long seed = 2026L;
        int queueCapacity = 20_000;

        List<Transaction> transactions = TransactionTasks.generateRandomTransactions(transactionCount, seed);

        Path csvPath = Path.of("results", "transaction_timings.csv");
        BenchmarkUtils.ensureCsvWithHeader(
                csvPath,
                "pattern,threads,time_ms,count,premium_count,total_converted_uah,total_cashback_uah,total_final_uah"
        );

        System.out.println("=== TRANSACTION PROCESSING EXPERIMENT ===");
        System.out.println("Transactions count: " + transactionCount);
        System.out.println("Queue capacity: " + queueCapacity);
        System.out.println();

        BenchmarkUtils.TimedValue<TransactionTasks.ProcessingSummary> sequential = BenchmarkUtils.measure(
                "Transactions-Sequential",
                () -> TransactionTasks.sequential(transactions)
        );
        printTransactionResult(sequential);
        saveTransactionCsv(csvPath, "sequential", 1, sequential);

        List<Integer> threadOptions = buildThreadOptions();

        for (int threads : threadOptions) {
            BenchmarkUtils.TimedValue<TransactionTasks.ProcessingSummary> producerConsumer = BenchmarkUtils.measure(
                    "Transactions-ProducerConsumer-" + threads,
                    () -> TransactionTasks.producerConsumer(transactions, threads, queueCapacity)
            );
            assertSummaryClose(sequential.getValue(), producerConsumer.getValue(), "producer_consumer", threads);
            printTransactionResult(producerConsumer);
            saveTransactionCsv(csvPath, "producer_consumer", threads, producerConsumer);

            BenchmarkUtils.TimedValue<TransactionTasks.ProcessingSummary> pipeline = BenchmarkUtils.measure(
                    "Transactions-Pipeline-" + threads,
                    () -> TransactionTasks.pipeline(transactions, threads, queueCapacity)
            );
            assertSummaryClose(sequential.getValue(), pipeline.getValue(), "pipeline", threads);
            printTransactionResult(pipeline);
            saveTransactionCsv(csvPath, "pipeline", threads, pipeline);

            System.out.println();
        }

        System.out.println("Готово. Результати збережено у: " + csvPath.toAbsolutePath());
    }

    private static List<Integer> buildThreadOptions() {
        int available = Runtime.getRuntime().availableProcessors();
        int[] candidates = {2, 4, 8, 16};

        List<Integer> result = new ArrayList<>();
        for (int value : candidates) {
            if (value <= available * 2) {
                result.add(value);
            }
        }

        if (result.isEmpty()) {
            result.add(2);
        }

        return result;
    }

    private static void saveStatsCsv(
            Path csvPath,
            String pattern,
            int threads,
            BenchmarkUtils.TimedValue<StatsResult> timed
    ) throws Exception {
        StatsResult r = timed.getValue();

        BenchmarkUtils.appendCsvRow(
                csvPath,
                pattern,
                String.valueOf(threads),
                String.valueOf(timed.getTimeMs()),
                BenchmarkUtils.fmt(r.getMin()),
                BenchmarkUtils.fmt(r.getMax()),
                BenchmarkUtils.fmt(r.getAverage()),
                BenchmarkUtils.fmt(r.getMedian()),
                String.valueOf(r.getSize())
        );
    }

    private static void saveHtmlCsv(
            Path csvPath,
            String pattern,
            int threads,
            int filesCount,
            BenchmarkUtils.TimedValue<Map<String, Integer>> timed
    ) throws Exception {
        Map<String, Integer> map = timed.getValue();

        BenchmarkUtils.appendCsvRow(
                csvPath,
                pattern,
                String.valueOf(threads),
                String.valueOf(timed.getTimeMs()),
                String.valueOf(filesCount),
                String.valueOf(map.size()),
                topTagsToString(map, 5)
        );
    }

    private static void saveMatrixCsv(
            Path csvPath,
            String pattern,
            int threads,
            int size,
            BenchmarkUtils.TimedValue<double[][]> timed,
            double checksum
    ) throws Exception {
        BenchmarkUtils.appendCsvRow(
                csvPath,
                pattern,
                String.valueOf(threads),
                String.valueOf(timed.getTimeMs()),
                String.valueOf(size),
                BenchmarkUtils.fmt(checksum)
        );
    }

    private static void saveTransactionCsv(
            Path csvPath,
            String pattern,
            int threads,
            BenchmarkUtils.TimedValue<TransactionTasks.ProcessingSummary> timed
    ) throws Exception {
        TransactionTasks.ProcessingSummary summary = timed.getValue();

        BenchmarkUtils.appendCsvRow(
                csvPath,
                pattern,
                String.valueOf(threads),
                String.valueOf(timed.getTimeMs()),
                String.valueOf(summary.getCount()),
                String.valueOf(summary.getPremiumCount()),
                BenchmarkUtils.fmt(summary.getTotalConvertedUah()),
                BenchmarkUtils.fmt(summary.getTotalCashbackUah()),
                BenchmarkUtils.fmt(summary.getTotalFinalUah())
        );
    }

    private static void printHtmlResult(BenchmarkUtils.TimedValue<Map<String, Integer>> timed) {
        Map<String, Integer> result = timed.getValue();

        System.out.println("[" + timed.getLabel() + "] "
                + timed.getTimeMs() + " ms | "
                + "uniqueTags=" + result.size() + " | "
                + "top5=" + topTagsToString(result, 5));
    }

    private static void printMatrixResult(BenchmarkUtils.TimedValue<double[][]> timed, double checksum) {
        double[][] matrix = timed.getValue();

        System.out.println("[" + timed.getLabel() + "] "
                + timed.getTimeMs() + " ms | "
                + "rows=" + matrix.length + ", "
                + "cols=" + matrix[0].length + ", "
                + "checksum=" + BenchmarkUtils.fmt(checksum));
    }

    private static void printTransactionResult(
            BenchmarkUtils.TimedValue<TransactionTasks.ProcessingSummary> timed
    ) {
        TransactionTasks.ProcessingSummary s = timed.getValue();

        System.out.println("[" + timed.getLabel() + "] "
                + timed.getTimeMs() + " ms | "
                + "count=" + s.getCount() + ", "
                + "premium=" + s.getPremiumCount() + ", "
                + "convertedUAH=" + BenchmarkUtils.fmt(s.getTotalConvertedUah()) + ", "
                + "cashbackUAH=" + BenchmarkUtils.fmt(s.getTotalCashbackUah()) + ", "
                + "finalUAH=" + BenchmarkUtils.fmt(s.getTotalFinalUah()));
    }

    private static String topTagsToString(Map<String, Integer> map, int limit) {
        List<Map.Entry<String, Integer>> top = HtmlTasks.topTags(map, limit);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < top.size(); i++) {
            if (i > 0) {
                sb.append("; ");
            }
            sb.append(top.get(i).getKey()).append("=").append(top.get(i).getValue());
        }

        return sb.toString();
    }

    private static void assertClose(double expected, double actual, String pattern, int threads) {
        double diff = Math.abs(expected - actual);
        double tolerance = 1e-6 * Math.max(1.0, Math.abs(expected));

        if (diff > tolerance) {
            throw new IllegalStateException(
                    "Невідповідність checksum для " + pattern + " (" + threads + " threads): "
                            + "expected=" + expected + ", actual=" + actual
            );
        }
    }

    private static void assertSummaryClose(
            TransactionTasks.ProcessingSummary expected,
            TransactionTasks.ProcessingSummary actual,
            String pattern,
            int threads
    ) {
        if (expected.getCount() != actual.getCount()) {
            throw new IllegalStateException(
                    "Невідповідність count для " + pattern + " (" + threads + " threads)"
            );
        }

        if (expected.getPremiumCount() != actual.getPremiumCount()) {
            throw new IllegalStateException(
                    "Невідповідність premiumCount для " + pattern + " (" + threads + " threads)"
            );
        }

        assertMoneyClose(expected.getTotalConvertedUah(), actual.getTotalConvertedUah(), pattern, threads, "converted");
        assertMoneyClose(expected.getTotalCashbackUah(), actual.getTotalCashbackUah(), pattern, threads, "cashback");
        assertMoneyClose(expected.getTotalFinalUah(), actual.getTotalFinalUah(), pattern, threads, "final");
    }

    private static void assertMoneyClose(
            double expected,
            double actual,
            String pattern,
            int threads,
            String field
    ) {
        double expectedRounded = Math.round(expected * 100.0) / 100.0;
        double actualRounded = Math.round(actual * 100.0) / 100.0;

        double diff = Math.abs(expectedRounded - actualRounded);
        double tolerance = 0.10;

        if (diff > tolerance) {
            throw new IllegalStateException(
                    "Невідповідність " + field + " для " + pattern + " (" + threads + " threads): "
                            + "expected=" + expectedRounded + ", actual=" + actualRounded
            );
        }
    }
}
