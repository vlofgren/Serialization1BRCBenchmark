package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class Bench_CustomFileChannel_Strings extends Benchmark {
    public Bench_CustomFileChannel_Strings() throws IOException {
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

                String city = measurement.city();
                byte[] cityBytes = city.getBytes();

                buffer.put((byte) cityBytes.length);
                buffer.put(cityBytes);
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

            Map<String, ResultsObserver> stats = new HashMap<>();

            try (var fc = (FileChannel) Files.newByteChannel(tempFile, StandardOpenOption.READ)) {

                buffer.flip();

                for (int i = 0; i < itemCount; i++) {

                    if (buffer.remaining() < 32) {
                        buffer.compact();
                        fc.read(buffer);
                        buffer.flip();
                    }

                    int len = buffer.get();
                    byte[] cityBytes = new byte[len];
                    buffer.get(cityBytes);
                    String city = new String(cityBytes);
                    int temperature = buffer.getShort();

                    stats.computeIfAbsent(city, k -> new ResultsObserver()).observe(temperature / 100.);
                }

                stats.forEach((city, stat) -> {
                    System.out.println("City: " + city + " " + stat);
                });
            }

            long readEnd = System.currentTimeMillis();
            System.out.println("Read time: " + (readEnd - readStart) + " ms");
            bestReadTime = Math.min(bestReadTime, readEnd - readStart);
        }

        return (int) bestReadTime;
    }
}
