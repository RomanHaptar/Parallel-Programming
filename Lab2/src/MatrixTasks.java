import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;

public final class MatrixTasks {

    private MatrixTasks() {
    }

    private static final class RowBlock {
        final int startRow;
        final double[][] values;

        RowBlock(int startRow, double[][] values) {
            this.startRow = startRow;
            this.values = values;
        }
    }

    public static double[][] generateRandomMatrix(int rows, int cols, long seed, double minValue, double maxValue) {
        if (rows <= 0 || cols <= 0) {
            throw new IllegalArgumentException("Розміри матриці мають бути > 0");
        }
        if (minValue >= maxValue) {
            throw new IllegalArgumentException("minValue має бути < maxValue");
        }

        Random random = new Random(seed);
        double[][] matrix = new double[rows][cols];
        double range = maxValue - minValue;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = minValue + random.nextDouble() * range;
            }
        }

        return matrix;
    }

    public static double[][] sequential(double[][] a, double[][] b) {
        validateMatrices(a, b);

        int rows = a.length;
        int cols = b[0].length;
        double[][] result = new double[rows][cols];
        double[][] bTransposed = transpose(b);

        multiplyRowRange(a, bTransposed, result, 0, rows);
        return result;
    }

    public static double[][] workerPool(double[][] a, double[][] b, int threads) throws Exception {
        validateMatrices(a, b);
        validateThreads(threads);

        int rows = a.length;
        int cols = b[0].length;
        double[][] bTransposed = transpose(b);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            int chunkSize = Math.max(1, (rows + threads - 1) / threads);
            List<Future<RowBlock>> futures = new ArrayList<>();

            for (int start = 0; start < rows; start += chunkSize) {
                int end = Math.min(start + chunkSize, rows);
                int from = start;
                int to = end;

                Callable<RowBlock> task = () -> {
                    double[][] partial = new double[to - from][cols];
                    multiplyRowRangeToPartial(a, bTransposed, partial, from, to);
                    return new RowBlock(from, partial);
                };
                futures.add(executor.submit(task));
            }

            double[][] result = new double[rows][cols];
            for (Future<RowBlock> future : futures) {
                RowBlock block = future.get();
                copyBlockToResult(result, block);
            }

            return result;
        } finally {
            executor.shutdown();
        }
    }

    public static double[][] forkJoin(double[][] a, double[][] b, int parallelism) {
        validateMatrices(a, b);
        validateThreads(parallelism);

        int rows = a.length;
        int cols = b[0].length;
        double[][] result = new double[rows][cols];
        double[][] bTransposed = transpose(b);

        int threshold = Math.max(10, rows / Math.max(1, parallelism * 2));

        ForkJoinPool pool = new ForkJoinPool(parallelism);
        try {
            pool.invoke(new MatrixMultiplyAction(a, bTransposed, result, 0, rows, threshold));
            return result;
        } finally {
            pool.shutdown();
        }
    }

    public static double[][] mapReduce(double[][] a, double[][] b, int threads) throws Exception {
        validateMatrices(a, b);
        validateThreads(threads);

        int rows = a.length;
        int cols = b[0].length;
        double[][] bTransposed = transpose(b);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            int chunkSize = Math.max(1, (rows + threads - 1) / threads);
            List<Future<RowBlock>> mapStage = new ArrayList<>();

            for (int start = 0; start < rows; start += chunkSize) {
                int end = Math.min(start + chunkSize, rows);
                int from = start;
                int to = end;

                mapStage.add(executor.submit(() -> {
                    double[][] partial = new double[to - from][cols];
                    multiplyRowRangeToPartial(a, bTransposed, partial, from, to);
                    return new RowBlock(from, partial);
                }));
            }

            List<RowBlock> mappedBlocks = new ArrayList<>();
            for (Future<RowBlock> future : mapStage) {
                mappedBlocks.add(future.get());
            }

            return reduce(mappedBlocks, rows, cols);
        } finally {
            executor.shutdown();
        }
    }

    public static double checksum(double[][] matrix) {
        validateMatrix(matrix);

        double sum = 0.0;
        for (double[] row : matrix) {
            for (double value : row) {
                sum += value;
            }
        }
        return sum;
    }

    private static double[][] reduce(List<RowBlock> blocks, int rows, int cols) {
        double[][] result = new double[rows][cols];
        for (RowBlock block : blocks) {
            copyBlockToResult(result, block);
        }
        return result;
    }

    private static void copyBlockToResult(double[][] result, RowBlock block) {
        for (int i = 0; i < block.values.length; i++) {
            System.arraycopy(block.values[i], 0, result[block.startRow + i], 0, block.values[i].length);
        }
    }

    private static void multiplyRowRange(double[][] a, double[][] bTransposed, double[][] result, int startRow, int endRow) {
        int cols = bTransposed.length;
        int common = a[0].length;

        for (int i = startRow; i < endRow; i++) {
            double[] aRow = a[i];
            double[] resultRow = result[i];

            for (int j = 0; j < cols; j++) {
                double[] bRow = bTransposed[j];
                double sum = 0.0;

                for (int k = 0; k < common; k++) {
                    sum += aRow[k] * bRow[k];
                }

                resultRow[j] = sum;
            }
        }
    }

    private static void multiplyRowRangeToPartial(
            double[][] a,
            double[][] bTransposed,
            double[][] partial,
            int startRow,
            int endRow
    ) {
        int cols = bTransposed.length;
        int common = a[0].length;

        for (int i = startRow; i < endRow; i++) {
            double[] aRow = a[i];
            double[] resultRow = partial[i - startRow];

            for (int j = 0; j < cols; j++) {
                double[] bRow = bTransposed[j];
                double sum = 0.0;

                for (int k = 0; k < common; k++) {
                    sum += aRow[k] * bRow[k];
                }

                resultRow[j] = sum;
            }
        }
    }

    private static double[][] transpose(double[][] matrix) {
        validateMatrix(matrix);

        int rows = matrix.length;
        int cols = matrix[0].length;
        double[][] transposed = new double[cols][rows];

        for (int i = 0; i < rows; i++) {
            if (matrix[i].length != cols) {
                throw new IllegalArgumentException("Матриця має бути прямокутною");
            }
            for (int j = 0; j < cols; j++) {
                transposed[j][i] = matrix[i][j];
            }
        }

        return transposed;
    }

    private static void validateMatrices(double[][] a, double[][] b) {
        validateMatrix(a);
        validateMatrix(b);

        if (a[0].length != b.length) {
            throw new IllegalArgumentException(
                    "Неможливо перемножити матриці: кількість стовпців A має дорівнювати кількості рядків B"
            );
        }
    }

    private static void validateMatrix(double[][] matrix) {
        if (matrix == null || matrix.length == 0 || matrix[0].length == 0) {
            throw new IllegalArgumentException("Матриця не може бути null або порожньою");
        }

        int cols = matrix[0].length;
        for (double[] row : matrix) {
            if (row == null || row.length != cols) {
                throw new IllegalArgumentException("Матриця має бути прямокутною");
            }
        }
    }

    private static void validateThreads(int threads) {
        if (threads <= 0) {
            throw new IllegalArgumentException("Кількість потоків має бути > 0");
        }
    }

    private static final class MatrixMultiplyAction extends RecursiveAction {
        private final double[][] a;
        private final double[][] bTransposed;
        private final double[][] result;
        private final int startRow;
        private final int endRow;
        private final int threshold;

        MatrixMultiplyAction(
                double[][] a,
                double[][] bTransposed,
                double[][] result,
                int startRow,
                int endRow,
                int threshold
        ) {
            this.a = a;
            this.bTransposed = bTransposed;
            this.result = result;
            this.startRow = startRow;
            this.endRow = endRow;
            this.threshold = threshold;
        }

        @Override
        protected void compute() {
            int length = endRow - startRow;
            if (length <= threshold) {
                multiplyRowRange(a, bTransposed, result, startRow, endRow);
                return;
            }

            int mid = startRow + length / 2;

            MatrixMultiplyAction left = new MatrixMultiplyAction(
                    a, bTransposed, result, startRow, mid, threshold
            );
            MatrixMultiplyAction right = new MatrixMultiplyAction(
                    a, bTransposed, result, mid, endRow, threshold
            );

            invokeAll(left, right);
        }
    }
}
