package benchmark;

import com.google.common.testing.GcFinalization;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.io.FuryInputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class Bench_Fury_String extends Benchmark {
    public Bench_Fury_String() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    record Measurement(String city, int temperature) {}

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path file = Files.createTempFile(tempDir, "cities", ".parquet");

        long writeStart = System.currentTimeMillis();

        Fury fury = Fury.builder()
                .withRefTracking(false)
                .withLanguage(Language.JAVA)
                .build();
        fury.register(Measurement.class);

        try (var dataStream = new BufferedOutputStream(Files.newOutputStream(file))) {
            for (int i = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();

                fury.serializeJavaObject(dataStream, new Measurement(measurement.city(), measurement.shortValue()));
            }
        }
        long writeEnd = System.currentTimeMillis();

        System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

        long bestReadTime = Integer.MAX_VALUE;
        for (int iter = 0; iter < iters; iter++) {
            Map<String, ResultsObserver> resultsObserverMap = new HashMap<>();
            GcFinalization.awaitFullGc();
            long readStart = System.currentTimeMillis();

            try (var furyInputStream = new FuryInputStream(Files.newInputStream(file))) {
                for (int i = 0; i < itemCount; i++) {
                    Measurement measurement = fury.deserializeJavaObject(furyInputStream, Measurement.class);
                    resultsObserverMap.computeIfAbsent(measurement.city(), k -> new ResultsObserver()).observe(measurement.temperature() / 100.);
                }
            }

            long readEnd = System.currentTimeMillis();

            resultsObserverMap.forEach((city, observer) -> {
                System.out.println(city + " " + observer);
            });

            System.out.println("Read time: " + (readEnd - readStart) + " ms");
            bestReadTime = Math.min(bestReadTime, readEnd - readStart);
        }
        System.out.println("Best read time: " + bestReadTime + " ms");

        return (int) bestReadTime;
    }
}
