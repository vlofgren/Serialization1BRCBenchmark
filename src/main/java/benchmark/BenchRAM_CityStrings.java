package benchmark;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class BenchRAM_CityStrings extends Benchmark {
    public BenchRAM_CityStrings() throws IOException {
        super(10_000_000);
    }

    protected int runBenchmark(int iters) throws IOException {
        String[] cityNames = new String[itemCount];
        short[] measurements = new short[itemCount];

        for (int i = 0; i < itemCount; i++) {
            var measurement = this.nextMeasurement();
            cityNames[i] = measurement.city();
            measurements[i] = measurement.shortValue();
        }

        int bestReadTime = Integer.MAX_VALUE;


        for (int iter = 0; iter < iters; iter++) {
            long startTime = System.currentTimeMillis();
            Map<String, ResultsObserver> observerMap = new HashMap<>();

            for (int i = 0; i < itemCount; i++) {
                observerMap.computeIfAbsent(cityNames[i], k -> new ResultsObserver())
                        .observe(measurements[i] / 100.);
            }

            observerMap.forEach((city, observer) -> {
                System.out.println(city + " " + observer);
            });

            bestReadTime = (int) Math.min(bestReadTime, System.currentTimeMillis() - startTime);
        }

        return bestReadTime;
    }
}
