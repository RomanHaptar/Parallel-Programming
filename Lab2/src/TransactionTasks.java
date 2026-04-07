import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

public final class TransactionTasks {

    private TransactionTasks() {
    }

    private static final String[] CURRENCIES = {"UAH", "USD", "EUR", "GBP", "PLN"};
    private static final String[] CATEGORIES = {
            "groceries", "electronics", "books", "clothes", "travel", "pharmacy"
    };

    private static final Transaction POISON = Transaction.poisonPill();

    public static final class ProcessingSummary {
        private final int count;
        private final int premiumCount;
        private final double totalConvertedUah;
        private final double totalCashbackUah;
        private final double totalFinalUah;

        public ProcessingSummary(
                int count,
                int premiumCount,
                double totalConvertedUah,
                double totalCashbackUah,
                double totalFinalUah
        ) {
            this.count = count;
            this.premiumCount = premiumCount;
            this.totalConvertedUah = totalConvertedUah;
            this.totalCashbackUah = totalCashbackUah;
            this.totalFinalUah = totalFinalUah;
        }

        public int getCount() {
            return count;
        }

        public int getPremiumCount() {
            return premiumCount;
        }

        public double getTotalConvertedUah() {
            return totalConvertedUah;
        }

        public double getTotalCashbackUah() {
            return totalCashbackUah;
        }

        public double getTotalFinalUah() {
            return totalFinalUah;
        }

        @Override
        public String toString() {
            return "ProcessingSummary{" +
                    "count=" + count +
                    ", premiumCount=" + premiumCount +
                    ", totalConvertedUah=" + totalConvertedUah +
                    ", totalCashbackUah=" + totalCashbackUah +
                    ", totalFinalUah=" + totalFinalUah +
                    '}';
        }
    }

    private static final class SummaryAccumulator {
        private final AtomicInteger count = new AtomicInteger();
        private final AtomicInteger premiumCount = new AtomicInteger();
        private final DoubleAdder totalConvertedUah = new DoubleAdder();
        private final DoubleAdder totalCashbackUah = new DoubleAdder();
        private final DoubleAdder totalFinalUah = new DoubleAdder();

        void add(Transaction tx) {
            count.incrementAndGet();
            if (tx.isPremiumUser()) {
                premiumCount.incrementAndGet();
            }
            totalConvertedUah.add(tx.getConvertedAmountUah());
            totalCashbackUah.add(tx.getCashbackAmountUah());
            totalFinalUah.add(tx.getFinalAmountUah());
        }

        ProcessingSummary toSummary() {
            return new ProcessingSummary(
                    count.get(),
                    premiumCount.get(),
                    round2(totalConvertedUah.sum()),
                    round2(totalCashbackUah.sum()),
                    round2(totalFinalUah.sum())
            );
        }
    }

    public static List<Transaction> generateRandomTransactions(int count, long seed) {
        if (count <= 0) {
            throw new IllegalArgumentException("Кількість транзакцій має бути > 0");
        }

        Random random = new Random(seed);
        List<Transaction> transactions = new ArrayList<>(count);

        for (int i = 1; i <= count; i++) {
            long transactionId = i;
            long userId = 1 + random.nextInt(10_000);
            double amount = round2(10.0 + random.nextDouble() * 50_000.0);
            String currency = CURRENCIES[random.nextInt(CURRENCIES.length)];
            String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
            boolean premiumUser = random.nextDouble() < 0.25;

            transactions.add(new Transaction(
                    transactionId,
                    userId,
                    amount,
                    currency,
                    category,
                    premiumUser
            ));
        }

        return transactions;
    }

    public static ProcessingSummary sequential(List<Transaction> transactions) {
        validateTransactions(transactions);

        SummaryAccumulator accumulator = new SummaryAccumulator();

        for (Transaction tx : transactions) {
            Transaction processed = processTransaction(tx);
            accumulator.add(processed);
        }

        return accumulator.toSummary();
    }

    public static ProcessingSummary producerConsumer(
            List<Transaction> transactions,
            int consumerThreads,
            int queueCapacity
    ) throws Exception {
        validateTransactions(transactions);
        validateThreads(consumerThreads);
        validateQueueCapacity(queueCapacity);

        BlockingQueue<Transaction> queue = new ArrayBlockingQueue<>(queueCapacity);
        SummaryAccumulator accumulator = new SummaryAccumulator();

        ExecutorService executor = Executors.newFixedThreadPool(consumerThreads + 1);

        try {
            Future<?> producerFuture = executor.submit(() -> {
                produceTransactions(transactions, queue, consumerThreads);
                return null;
            });

            List<Future<?>> consumerFutures = new ArrayList<>();
            for (int i = 0; i < consumerThreads; i++) {
                consumerFutures.add(executor.submit(() -> {
                    consumeAndProcess(queue, accumulator);
                    return null;
                }));
            }

            producerFuture.get();
            for (Future<?> future : consumerFutures) {
                future.get();
            }

            return accumulator.toSummary();
        } finally {
            shutdownExecutor(executor);
        }
    }

    public static ProcessingSummary pipeline(
            List<Transaction> transactions,
            int workersPerStage,
            int queueCapacity
    ) throws Exception {
        validateTransactions(transactions);
        validateThreads(workersPerStage);
        validateQueueCapacity(queueCapacity);

        BlockingQueue<Transaction> inputQueue = new ArrayBlockingQueue<>(queueCapacity);
        BlockingQueue<Transaction> convertedQueue = new ArrayBlockingQueue<>(queueCapacity);
        BlockingQueue<Transaction> finalQueue = new ArrayBlockingQueue<>(queueCapacity);

        ExecutorService executor = Executors.newFixedThreadPool(2 * workersPerStage + 2);

        try {
            Future<?> producerFuture = executor.submit(() -> {
                produceTransactions(transactions, inputQueue, workersPerStage);
                return null;
            });

            List<Future<?>> converterFutures = new ArrayList<>();
            for (int i = 0; i < workersPerStage; i++) {
                converterFutures.add(executor.submit(() -> {
                    convertStage(inputQueue, convertedQueue);
                    return null;
                }));
            }

            List<Future<?>> cashbackFutures = new ArrayList<>();
            for (int i = 0; i < workersPerStage; i++) {
                cashbackFutures.add(executor.submit(() -> {
                    cashbackStage(convertedQueue, finalQueue);
                    return null;
                }));
            }

            Future<ProcessingSummary> aggregatorFuture = executor.submit(
                    () -> aggregateStage(finalQueue, workersPerStage)
            );

            producerFuture.get();

            for (Future<?> future : converterFutures) {
                future.get();
            }

            for (Future<?> future : cashbackFutures) {
                future.get();
            }

            return aggregatorFuture.get();
        } finally {
            shutdownExecutor(executor);
        }
    }

    private static void produceTransactions(
            List<Transaction> transactions,
            BlockingQueue<Transaction> queue,
            int poisonCount
    ) throws InterruptedException {
        for (Transaction tx : transactions) {
            queue.put(tx);
        }

        for (int i = 0; i < poisonCount; i++) {
            queue.put(POISON);
        }
    }

    private static void consumeAndProcess(
            BlockingQueue<Transaction> queue,
            SummaryAccumulator accumulator
    ) throws InterruptedException {
        while (true) {
            Transaction tx = queue.take();

            if (tx.isPoisonPill()) {
                break;
            }

            Transaction processed = processTransaction(tx);
            accumulator.add(processed);
        }
    }

    private static void convertStage(
            BlockingQueue<Transaction> inputQueue,
            BlockingQueue<Transaction> convertedQueue
    ) throws InterruptedException {
        while (true) {
            Transaction tx = inputQueue.take();

            if (tx.isPoisonPill()) {
                convertedQueue.put(POISON);
                break;
            }

            Transaction converted = convertToUah(tx);
            convertedQueue.put(converted);
        }
    }

    private static void cashbackStage(
            BlockingQueue<Transaction> convertedQueue,
            BlockingQueue<Transaction> finalQueue
    ) throws InterruptedException {
        while (true) {
            Transaction tx = convertedQueue.take();

            if (tx.isPoisonPill()) {
                finalQueue.put(POISON);
                break;
            }

            Transaction processed = applyCashback(tx);
            finalQueue.put(processed);
        }
    }

    private static ProcessingSummary aggregateStage(
            BlockingQueue<Transaction> finalQueue,
            int poisonCount
    ) throws InterruptedException {
        SummaryAccumulator accumulator = new SummaryAccumulator();
        int poisonSeen = 0;

        while (poisonSeen < poisonCount) {
            Transaction tx = finalQueue.take();

            if (tx.isPoisonPill()) {
                poisonSeen++;
            } else {
                accumulator.add(tx);
            }
        }

        return accumulator.toSummary();
    }

    private static Transaction processTransaction(Transaction tx) {
        return applyCashback(convertToUah(tx));
    }

    private static Transaction convertToUah(Transaction tx) {
        double rate = getExchangeRateToUah(tx.getCurrency());
        double converted = round2(tx.getAmount() * rate);
        return tx.withConvertedAmountUah(converted);
    }

    private static Transaction applyCashback(Transaction tx) {
        double cashbackRate = getCashbackRate(tx.getCategory(), tx.isPremiumUser());
        double cashback = round2(tx.getConvertedAmountUah() * cashbackRate);
        return tx.withCashbackAmountUah(cashback);
    }

    private static double getExchangeRateToUah(String currency) {
        String code = currency == null ? "" : currency.trim().toUpperCase(Locale.ROOT);

        switch (code) {
            case "UAH":
                return 1.0;
            case "USD":
                return 39.50;
            case "EUR":
                return 43.20;
            case "GBP":
                return 50.10;
            case "PLN":
                return 10.05;
            default:
                return 1.0;
        }
    }

    private static double getCashbackRate(String category, boolean premiumUser) {
        String value = category == null ? "" : category.trim().toLowerCase(Locale.ROOT);

        if (premiumUser) {
            switch (value) {
                case "groceries":
                    return 0.05;
                case "electronics":
                    return 0.02;
                case "books":
                    return 0.04;
                case "pharmacy":
                    return 0.03;
                case "travel":
                    return 0.015;
                case "clothes":
                    return 0.025;
                default:
                    return 0.02;
            }
        } else {
            switch (value) {
                case "groceries":
                    return 0.02;
                case "electronics":
                    return 0.01;
                case "books":
                    return 0.015;
                case "pharmacy":
                    return 0.01;
                case "travel":
                    return 0.005;
                case "clothes":
                    return 0.01;
                default:
                    return 0.005;
            }
        }
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static void validateTransactions(List<Transaction> transactions) {
        if (transactions == null || transactions.isEmpty()) {
            throw new IllegalArgumentException("Список транзакцій не може бути null або порожнім");
        }
    }

    private static void validateThreads(int threads) {
        if (threads <= 0) {
            throw new IllegalArgumentException("Кількість потоків має бути > 0");
        }
    }

    private static void validateQueueCapacity(int queueCapacity) {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("Розмір черги має бути > 0");
        }
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
