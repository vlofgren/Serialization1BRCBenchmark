package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class Bench_DataInputStream_Interleaved extends Benchmark {
    public Bench_DataInputStream_Interleaved() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path file = Files.createTempFile(tempDir, "cities", ".bin");

        long writeStart = System.currentTimeMillis();

        try (var dataStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            for (int i = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();

                dataStream.writeUTF(measurement.city());
                dataStream.writeShort(measurement.shortValue());
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

            try (var dataStream = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {

                while (true) {
                    resultsObserverMap.computeIfAbsent(dataStream.readUTF(), k -> new ResultsObserver()).observe(dataStream.readShort() / 100.);
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
