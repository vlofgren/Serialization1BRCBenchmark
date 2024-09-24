package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class Bench_ObjectInputStream extends Benchmark {
    public Bench_ObjectInputStream() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path file = Files.createTempFile(tempDir, "cities", ".bin");

        long writeStart = System.currentTimeMillis();

        try (var stream = new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            for (int i = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();

                stream.writeObject(new JavaSerializableMeasurement(measurement.city(), measurement.shortValue()));
                stream.reset(); // to avoid keeping references to all objects in memory
            }
        }

        long writeEnd = System.currentTimeMillis();

        System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

        int bestReadTime = Integer.MAX_VALUE;

        for (int iter = 0; iter < iters; iter++) {
            // read in a loop to let the VM and caches warm up
            GcFinalization.awaitFullGc();
            long readStart = System.currentTimeMillis();

            Map<String, ResultsObserver> resultsObserverMap = new HashMap<>();

            try (var stream = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
                while (true) {
                    if (stream.readObject() instanceof JavaSerializableMeasurement obj) {
                        resultsObserverMap.computeIfAbsent(obj.city, k -> new ResultsObserver()).observe(obj.temperature / 100.);
                    }
                    else {
                        throw new IllegalStateException("Bad data");
                    }
                }
            } catch (EOFException ex) {} // ignore
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            resultsObserverMap.forEach((city, observer) -> {
                System.out.println(city + " " + observer);
            });

            bestReadTime = (int) Math.min(bestReadTime, System.currentTimeMillis() - readStart);
        }

        System.out.println("Best read time: " + bestReadTime + " ms");
        return bestReadTime;
    }

    record JavaSerializableMeasurement(String city, int temperature) implements Serializable { }
}
