import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class HtmlTasks {

    private HtmlTasks() {
    }

    /**
     * Простий regex для відкривальних HTML-тегів.
     * Ігноруємо:
     * - закривальні теги </tag>
     * - <!DOCTYPE ...>
     * - <!-- comments -->
     * - <?xml ... ?>
     */
    private static final Pattern OPENING_TAG_PATTERN =
            Pattern.compile("<\\s*([a-zA-Z][a-zA-Z0-9]*)\\b[^>]*>");

    public static List<Path> loadHtmlFiles(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("Шлях до папки HTML не може бути null");
        }
        if (!Files.exists(directory)) {
            throw new IllegalArgumentException("Папка не існує: " + directory.toAbsolutePath());
        }
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("Шлях не є папкою: " + directory.toAbsolutePath());
        }

        List<Path> files = new ArrayList<>();

        try (Stream<Path> stream = Files.walk(directory)) {
            stream.filter(Files::isRegularFile)
                    .filter(HtmlTasks::isHtmlFile)
                    .sorted()
                    .forEach(files::add);
        }

        if (files.isEmpty()) {
            throw new IllegalArgumentException("У папці немає HTML-файлів: " + directory.toAbsolutePath());
        }

        return files;
    }

    public static Map<String, Integer> sequential(List<Path> files) throws IOException {
        validateFiles(files);

        Map<String, Integer> total = new HashMap<>();
        for (Path file : files) {
            Map<String, Integer> partial = countTagsInFile(file);
            mergeInto(total, partial);
        }
        return sortByCountDescThenName(total);
    }

    public static Map<String, Integer> workerPool(List<Path> files, int threads) throws Exception {
        validateFiles(files);
        validateThreads(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            int chunkSize = Math.max(1, (files.size() + threads - 1) / threads);
            List<Future<Map<String, Integer>>> futures = new ArrayList<>();

            for (int start = 0; start < files.size(); start += chunkSize) {
                int end = Math.min(start + chunkSize, files.size());
                int from = start;
                int to = end;

                Callable<Map<String, Integer>> task = () -> countTagsInRange(files, from, to);
                futures.add(executor.submit(task));
            }

            Map<String, Integer> total = new HashMap<>();
            for (Future<Map<String, Integer>> future : futures) {
                mergeInto(total, future.get());
            }

            return sortByCountDescThenName(total);
        } finally {
            executor.shutdown();
        }
    }

    public static Map<String, Integer> forkJoin(List<Path> files, int parallelism) {
        validateFiles(files);
        validateThreads(parallelism);

        int threshold = Math.max(10, files.size() / Math.max(1, parallelism * 2));

        ForkJoinPool pool = new ForkJoinPool(parallelism);
        try {
            Map<String, Integer> total = pool.invoke(new HtmlRecursiveTask(files, 0, files.size(), threshold));
            return sortByCountDescThenName(total);
        } finally {
            pool.shutdown();
        }
    }

    public static Map<String, Integer> mapReduce(List<Path> files, int threads) throws Exception {
        validateFiles(files);
        validateThreads(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            int chunkSize = Math.max(1, (files.size() + threads - 1) / threads);
            List<Future<Map<String, Integer>>> mapStage = new ArrayList<>();

            for (int start = 0; start < files.size(); start += chunkSize) {
                int end = Math.min(start + chunkSize, files.size());
                int from = start;
                int to = end;

                mapStage.add(executor.submit(() -> countTagsInRange(files, from, to)));
            }

            List<Map<String, Integer>> mappedResults = new ArrayList<>();
            for (Future<Map<String, Integer>> future : mapStage) {
                mappedResults.add(future.get());
            }

            Map<String, Integer> reduced = reduce(mappedResults);
            return sortByCountDescThenName(reduced);
        } finally {
            executor.shutdown();
        }
    }

    public static List<Map.Entry<String, Integer>> topTags(Map<String, Integer> tagCounts, int limit) {
        if (tagCounts == null || tagCounts.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map.Entry<String, Integer>> entries = new ArrayList<>(tagCounts.entrySet());
        entries.sort((a, b) -> {
            int byCount = Integer.compare(b.getValue(), a.getValue());
            if (byCount != 0) {
                return byCount;
            }
            return a.getKey().compareToIgnoreCase(b.getKey());
        });

        if (limit < entries.size()) {
            return entries.subList(0, limit);
        }
        return entries;
    }

    private static boolean isHtmlFile(Path path) {
        String name = path.getFileName().toString().toLowerCase();
        return name.endsWith(".html") || name.endsWith(".htm");
    }

    private static Map<String, Integer> reduce(List<Map<String, Integer>> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("Немає що зменшувати у reduce");
        }

        Map<String, Integer> total = new HashMap<>();
        for (Map<String, Integer> part : parts) {
            mergeInto(total, part);
        }
        return total;
    }

    private static Map<String, Integer> countTagsInRange(List<Path> files, int startInclusive, int endExclusive) throws IOException {
        Map<String, Integer> result = new HashMap<>();
        for (int i = startInclusive; i < endExclusive; i++) {
            Map<String, Integer> partial = countTagsInFile(files.get(i));
            mergeInto(result, partial);
        }
        return result;
    }

    private static Map<String, Integer> countTagsInFile(Path file) throws IOException {
        String content = Files.readString(file, StandardCharsets.UTF_8);
        Matcher matcher = OPENING_TAG_PATTERN.matcher(content);

        Map<String, Integer> counts = new HashMap<>();
        while (matcher.find()) {
            String tag = matcher.group(1).toLowerCase();
            counts.merge(tag, 1, Integer::sum);
        }
        return counts;
    }

    private static void mergeInto(Map<String, Integer> target, Map<String, Integer> source) {
        for (Map.Entry<String, Integer> entry : source.entrySet()) {
            target.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
    }

    private static Map<String, Integer> sortByCountDescThenName(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(map.entrySet());
        entries.sort((a, b) -> {
            int byCount = Integer.compare(b.getValue(), a.getValue());
            if (byCount != 0) {
                return byCount;
            }
            return a.getKey().compareToIgnoreCase(b.getKey());
        });

        Map<String, Integer> result = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static void validateFiles(List<Path> files) {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("Список HTML-файлів не може бути null або порожнім");
        }
    }

    private static void validateThreads(int threads) {
        if (threads <= 0) {
            throw new IllegalArgumentException("Кількість потоків має бути > 0");
        }
    }

    private static final class HtmlRecursiveTask extends RecursiveTask<Map<String, Integer>> {
        private final List<Path> files;
        private final int start;
        private final int end;
        private final int threshold;

        HtmlRecursiveTask(List<Path> files, int start, int end, int threshold) {
            this.files = files;
            this.start = start;
            this.end = end;
            this.threshold = threshold;
        }

        @Override
        protected Map<String, Integer> compute() {
            int length = end - start;
            if (length <= threshold) {
                try {
                    return countTagsInRange(files, start, end);
                } catch (IOException e) {
                    throw new RuntimeException("Помилка читання HTML-файлів", e);
                }
            }

            int mid = start + length / 2;

            HtmlRecursiveTask leftTask = new HtmlRecursiveTask(files, start, mid, threshold);
            HtmlRecursiveTask rightTask = new HtmlRecursiveTask(files, mid, end, threshold);

            leftTask.fork();
            Map<String, Integer> right = rightTask.compute();
            Map<String, Integer> left = leftTask.join();

            Map<String, Integer> merged = new HashMap<>();
            mergeInto(merged, left);
            mergeInto(merged, right);
            return merged;
        }
    }
}
