package benchmark;

class ResultsObserver {
    private double max = Double.MIN_VALUE;
    private double min = Double.MAX_VALUE;
    private double sum = 0;
    private int count = 0;

    void observe(double value) {
        max = Math.max(max, value);
        min = Math.min(min, value);
        sum += value;
        count++;
    }

    double average() {
        return sum / count;
    }

    double max() {
        return max;
    }

    double min() {
        return min;
    }

    static ResultsObserver merge(ResultsObserver a, ResultsObserver b) {
        ResultsObserver result = new ResultsObserver();
        result.max = Math.max(a.max, b.max);
        result.min = Math.min(a.min, b.min);
        result.sum = a.sum + b.sum;
        result.count = a.count + b.count;
        return result;
    }

    public String toString() {
        return "{max=" + max + ", min=" + min + ", avg=" + average() + "}";
    }
}
