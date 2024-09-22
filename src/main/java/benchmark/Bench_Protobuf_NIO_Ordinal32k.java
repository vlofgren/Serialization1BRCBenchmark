package benchmark;

import benchmark.proto.Onebrc;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/** Same as Bench_Protobuf_NIO_Ordinal, but with 32k buffer */
class Bench_Protobuf_NIO_Ordinal32k extends Benchmark {

    public Bench_Protobuf_NIO_Ordinal32k() throws IOException {
        super(10_000_000);
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path tempFile = Files.createTempFile(tempDir, "cities", ".bin");
        ByteBuffer buffer0 = ByteBuffer.allocate(1);
        ByteBuffer buffer = ByteBuffer.allocate(32*1024);

        long writeStart = System.currentTimeMillis();

        // this write code is very slow, but I can't find any examples on how to do this in a fast fashion,
        // so I'm leaving it as is, as we're benchmarking read times

        try (var fc = (FileChannel) Files.newByteChannel(tempFile, StandardOpenOption.WRITE)) {

            for (int i  = 0; i < itemCount; i++) {

                buffer0.clear();
                buffer.clear();

                var cc = CodedOutputStream.newInstance(buffer);

                var measurement = nextMeasurement();

                var record = Onebrc.OneBrcProtoRecordOrdinalCity.newBuilder()
                        .setCity(measurement.cityOrd())
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
            long readStart = System.currentTimeMillis();

            ResultsObserver[] stats = new ResultsObserver[citiesAll.size()];
            Arrays.setAll(stats, i -> new ResultsObserver());

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
                    var record = Onebrc.OneBrcProtoRecordOrdinalCity.parseFrom(cis);
                    buffer.limit(lim);
                    buffer.position(buffer.position() + size);

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
