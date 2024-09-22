package benchmark;

import benchmark.proto.Onebrc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class Bench_Protobuf_InputStream_String extends Benchmark {
    public Bench_Protobuf_InputStream_String() throws IOException {
        super(10_000_000);
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "cities", ".bin");

        long writeStart = System.currentTimeMillis();

        try (var fos = new BufferedOutputStream(Files.newOutputStream(tempFile))) {
            for (int i  = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();
                Onebrc.OneBrcProtoRecord
                        .newBuilder().setCity(measurement.city())
                        .setTemperature(measurement.shortValue())
                        .build().writeDelimitedTo(fos);
            }
        }

        long writeEnd = System.currentTimeMillis();
        System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

        long bestReadTime = Long.MAX_VALUE;

        // read in a loop to let the VM and caches warm up
        for (int iter = 0; iter < 3; iter++) {
            long readStart = System.currentTimeMillis();

            Map<String, ResultsObserver> stats = new HashMap<>();

            try (var fis = new BufferedInputStream(Files.newInputStream(tempFile))) {

                for (int i = 0; i < itemCount; i++) {
                    Onebrc.OneBrcProtoRecord record = Onebrc.OneBrcProtoRecord.parseDelimitedFrom(fis);
                    stats.computeIfAbsent(record.getCity(), k -> new ResultsObserver()).observe(record.getTemperature() / 100.);
                }

                stats.forEach((city, stat) -> {
                    System.out.println("City: " + city + " " + stat);
                });
            }

            long readEnd = System.currentTimeMillis();
            System.out.println("Read time: " + (readEnd - readStart) + " ms");
            bestReadTime = Math.min(bestReadTime, readEnd - readStart);
        }
        System.out.println("Best read time: " + bestReadTime + " ms");

        return (int) bestReadTime;
    }
}
