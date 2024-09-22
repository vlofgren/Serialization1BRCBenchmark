package benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class BenchRAM_CityOrdinals extends Benchmark {
    public BenchRAM_CityOrdinals() throws IOException {
        super(10_000_000);
    }

    protected int runBenchmark(int iters) throws IOException {
        int[] cityOrdinals = new int[itemCount];
        short[] measurements = new short[itemCount];
        List<String> cities = new ArrayList<>(citiesAll);

        for (int i = 0; i < itemCount; i++) {
            var measurement = this.nextMeasurement();
            cityOrdinals[i] = measurement.cityOrd();
            measurements[i] = measurement.shortValue();
        }

        int bestReadTime = Integer.MAX_VALUE;


        for (int iter = 0; iter < iters; iter++) {
            long startTime = System.currentTimeMillis();
            List<String> citiesValues = cities;
            ResultsObserver observers[] = new ResultsObserver[citiesValues.size()];
            Arrays.setAll(observers, i -> new ResultsObserver());

            for (int i = 0; i < itemCount; i++) {
                observers[cityOrdinals[i]].observe(measurements[i] / 100.);
            }
            for (int i = 0; i < cities.size(); i++) {
                System.out.println(cities.get(i) + " " + observers[i]);
            }

            bestReadTime = (int) Math.min(bestReadTime, System.currentTimeMillis() - startTime);
        }

        return bestReadTime;
    }
}
