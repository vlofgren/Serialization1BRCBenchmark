package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class Bench_DataOutputStream_Columnar extends Benchmark {
    public Bench_DataOutputStream_Columnar() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path citiesFile = Files.createTempFile(tempDir, "cities", ".bin");
        Path temperaturesFile = Files.createTempFile(tempDir, "temperatures", ".bin");

        long writeStart = System.currentTimeMillis();

        try (var citiesDos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(citiesFile)));
             var temperaturesDos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(temperaturesFile)))) {
            for (int i = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();
                citiesDos.writeUTF(measurement.city());
                temperaturesDos.writeShort(measurement.shortValue());
            }
        }

        long writeEnd = System.currentTimeMillis();

        System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

        int bestReadTime = Integer.MAX_VALUE;

        for (int iter = 0; iter < iters; iter++) {
            GcFinalization.awaitFullGc();
            // read in a loop to let the VM and caches warm up
            long readStart = System.currentTimeMillis();

            Map<String, ResultsObserver> resultsObserverMap = new HashMap<>();

            try (var citiesDis = new DataInputStream(new BufferedInputStream(Files.newInputStream(citiesFile)));
                 var temperaturesDis = new DataInputStream(new BufferedInputStream(Files.newInputStream(temperaturesFile)))) {

                while (true) {
                    int temperature = temperaturesDis.readShort();
                    resultsObserverMap.computeIfAbsent(citiesDis.readUTF(), k -> new ResultsObserver()).observe(temperature / 100.);
                }
            } catch (EOFException ex) {
            } // ignore

            resultsObserverMap.forEach((city, observer) -> {
                System.out.println(city + " " + observer);
            });

            bestReadTime = (int) Math.min(bestReadTime, System.currentTimeMillis() - readStart);
        }

        System.out.println("Best read time: " + bestReadTime + " ms");
        return bestReadTime;
    }
}
