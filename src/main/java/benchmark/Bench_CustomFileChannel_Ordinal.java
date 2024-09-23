package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class Bench_CustomFileChannel_Ordinal extends Benchmark {
    public Bench_CustomFileChannel_Ordinal() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "cities", ".bin");

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        long writeStart = System.currentTimeMillis();


        try (var fc = (FileChannel) Files.newByteChannel(tempFile, StandardOpenOption.WRITE)) {

            for (int i  = 0; i < itemCount; i++) {
                if (buffer.remaining() < 32) {
                    buffer.flip();
                    while (buffer.hasRemaining())
                        fc.write(buffer);
                    buffer.clear();
                }

                var measurement = nextMeasurement();

                buffer.put((byte) measurement.cityOrd());
                buffer.putShort(measurement.shortValue());
            }

            buffer.flip();
            while (buffer.hasRemaining())
                fc.write(buffer);
            buffer.clear();
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

            try (var fc = (FileChannel) Files.newByteChannel(tempFile, StandardOpenOption.READ)) {

                buffer.flip();

                for (int i = 0; i < itemCount; i++) {

                    if (buffer.remaining() < 32) {
                        buffer.compact();
                        fc.read(buffer);
                        buffer.flip();
                    }

                    int cityOrd = buffer.get();
                    int temperature = buffer.getShort();

                    stats[cityOrd].observe(temperature / 100.);
                }

                for (int i = 0; i < citiesAll.size(); i++) {
                    System.out.println("City: " + citiesAll.get(i) + " " + stats[i]);
                }
            }

            long readEnd = System.currentTimeMillis();
            System.out.println("Read time: " + (readEnd - readStart) + " ms");
            bestReadTime = Math.min(bestReadTime, readEnd - readStart);
        }

        return (int) bestReadTime;
    }
}
