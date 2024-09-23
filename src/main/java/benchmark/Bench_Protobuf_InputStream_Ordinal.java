package benchmark;

import benchmark.proto.Onebrc;
import com.google.common.testing.GcFinalization;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

class Bench_Protobuf_InputStream_Ordinal extends Benchmark {
    public Bench_Protobuf_InputStream_Ordinal() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "cities", ".bin");

        long writeStart = System.currentTimeMillis();

        try (var fos = new BufferedOutputStream(Files.newOutputStream(tempFile))) {
            for (int i  = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();
                Onebrc.OneBrcProtoRecordOrdinalCity.newBuilder().setCity(measurement.cityOrd())
                        .setTemperature(measurement.shortValue())
                        .build().writeDelimitedTo(fos);
            }
        }

        long writeEnd = System.currentTimeMillis();
        System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

        long bestReadTime = Long.MAX_VALUE;

        // read in a loop to let the VM and caches warm up
        for (int iter = 0; iter < 3; iter++) {
            GcFinalization.awaitFullGc();

            long readStart = System.currentTimeMillis();

            ResultsObserver[] stats = new ResultsObserver[citiesAll.size()];
            Arrays.setAll(stats, i -> new ResultsObserver());

            try (var fis = new BufferedInputStream(Files.newInputStream(tempFile))) {

                for (int i = 0; i < itemCount; i++) {
                    Onebrc.OneBrcProtoRecordOrdinalCity record = Onebrc.OneBrcProtoRecordOrdinalCity.parseDelimitedFrom(fis);
                    stats[record.getCity()].observe(record.getTemperature() / 100.);
                }

                for (int i = 0; i < citiesAll.size(); i++) {
                    System.out.println("City: " + citiesAll.get(i) + " " + stats[i]);
                }
            }

            long readEnd = System.currentTimeMillis();
            System.out.println("Read time: " + (readEnd - readStart) + " ms");
            bestReadTime = Math.min(bestReadTime, readEnd - readStart);
        }

        System.out.println("Best read time: " + bestReadTime + " ms");
        return (int) bestReadTime;
    }
}
