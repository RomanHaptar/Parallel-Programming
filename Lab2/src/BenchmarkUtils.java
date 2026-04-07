import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BenchmarkUtils {

    private BenchmarkUtils() {
    }

    @FunctionalInterface
    public interface Task<T> {
        T execute() throws Exception;
    }

    public static final class TimedValue<T> {
        private final String label;
        private final T value;
        private final long timeMs;

        public TimedValue(String label, T value, long timeMs) {
            this.label = label;
            this.value = value;
            this.timeMs = timeMs;
        }

        public String getLabel() {
            return label;
        }

        public T getValue() {
            return value;
        }

        public long getTimeMs() {
            return timeMs;
        }
    }

    public static <T> TimedValue<T> measure(String label, Task<T> task) throws Exception {
        long start = System.nanoTime();
        T value = task.execute();
        long end = System.nanoTime();
        long timeMs = (end - start) / 1_000_000;
        return new TimedValue<>(label, value, timeMs);
    }

    public static void ensureCsvWithHeader(Path csvPath, String header) throws IOException {
        if (csvPath.getParent() != null) {
            Files.createDirectories(csvPath.getParent());
        }
        if (!Files.exists(csvPath)) {
            Files.writeString(csvPath, header + System.lineSeparator(), StandardCharsets.UTF_8);
        }
    }

    public static void appendCsvRow(Path csvPath, String... values) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escapeCsv(values[i]));
        }
        sb.append(System.lineSeparator());
        Files.writeString(
                csvPath,
                sb.toString(),
                StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND
        );
    }

    private static String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        boolean mustQuote = value.contains(",") || value.contains("\"") || value.contains("\n");
        String escaped = value.replace("\"", "\"\"");
        return mustQuote ? "\"" + escaped + "\"" : escaped;
    }

    public static String fmt(double value) {
        return String.format(java.util.Locale.US, "%.4f", value);
    }

    public static void printTimedStats(TimedValue<StatsResult> timed) {
        StatsResult r = timed.getValue();
        System.out.println("[" + timed.getLabel() + "] "
                + timed.getTimeMs() + " ms | "
                + "min=" + fmt(r.getMin()) + ", "
                + "max=" + fmt(r.getMax()) + ", "
                + "avg=" + fmt(r.getAverage()) + ", "
                + "median=" + fmt(r.getMedian()) + ", "
                + "size=" + r.getSize());
    }
}
