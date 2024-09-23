package benchmark;

import benchmark.proto.Onebrc;
import com.google.common.testing.GcFinalization;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/** Protobuf with NIO and String city names */
class Bench_Protobuf_NIO_String extends Benchmark {
    public Bench_Protobuf_NIO_String() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "cities", ".bin");
        ByteBuffer buffer0 = ByteBuffer.allocate(1);
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        long writeStart = System.currentTimeMillis();

        // this write code is very slow, but I can't find any examples on how to do this in a fast fashion,
        // so I'm leaving it as is, as we're benchmarking read times
        try (var fc = (FileChannel) Files.newByteChannel(tempFile, StandardOpenOption.WRITE)) {

            for (int i  = 0; i < itemCount; i++) {

                buffer0.clear();
                buffer.clear();

                var cc = CodedOutputStream.newInstance(buffer);

                var measurement = nextMeasurement();

                var record = Onebrc.OneBrcProtoRecord.newBuilder().setCity(measurement.city())
                        .setTemperature(measurement.shortValue())
                        .build();

                buffer0.put((byte) record.getSerializedSize());
                buffer0.flip();

                record.writeTo(cc);
                cc.flush();
                buffer.flip();
                fc.write(new ByteBuffer[]{ buffer0, buffer });

            }
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

                for (int i = 0; i < itemCount; i++) {

                    int size;
                    if (!buffer.hasRemaining()) {
                        buffer.clear();
                        fc.read(buffer);
                        buffer.flip();
                    }
                    size = buffer.get();

                    if (buffer.remaining() < size) {
                        buffer.compact();
                        fc.read(buffer);
                        buffer.flip();
                    }

                    int lim = buffer.limit();
                    buffer.limit(buffer.position() + size);
                    var cis = CodedInputStream.newInstance(buffer);
                    var record = Onebrc.OneBrcProtoRecord.parseFrom(cis);
                    buffer.limit(lim);
                    buffer.position(buffer.position() + size);

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
