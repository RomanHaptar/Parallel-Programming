public class StatsResult {
    private final double min;
    private final double max;
    private final double average;
    private final double median;
    private final int size;

    public StatsResult(double min, double max, double average, double median, int size) {
        this.min = min;
        this.max = max;
        this.average = average;
        this.median = median;
        this.size = size;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAverage() {
        return average;
    }

    public double getMedian() {
        return median;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "StatsResult{" +
                "min=" + min +
                ", max=" + max +
                ", average=" + average +
                ", median=" + median +
                ", size=" + size +
                '}';
    }
}
