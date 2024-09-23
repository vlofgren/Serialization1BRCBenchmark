package benchmark;

import blue.strategic.parquet.*;
import com.google.common.testing.GcFinalization;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

class Bench_Parquet extends Benchmark {
    public Bench_Parquet() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {
        Path file = Files.createTempFile(tempDir, "cities", ".parquet");

        long writeStart = System.currentTimeMillis();

        try (var writer = ParquetWriter.writeFile(
                OneBRCParquetRecord.schema,
                file.toFile(),
                OneBRCParquetRecord::dehydrate))
        {
            for (int i  = 0; i < itemCount; i++) {
                var measurement = nextMeasurement();
                writer.write(new OneBRCParquetRecord(
                        measurement.city(),
                        measurement.shortValue()
                ));
            }
        }
        long writeEnd = System.currentTimeMillis();

        System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

        long bestReadTime = Integer.MAX_VALUE;
        for (int iter = 0; iter < iters; iter++) {
            GcFinalization.awaitFullGc();
            long readStart = System.currentTimeMillis();

            try (var stream = ParquetReader.streamContent(file.toFile(),
                    HydratorSupplier.constantly(new OneBRCParquetHydrator()))) {

                stream.collect(Collectors.groupingBy(r -> r.city, Collectors.summarizingDouble(r -> r.temperature / 100.)))
                        .forEach((city, stats) -> {
                            System.out.println("City: " + city + " " + stats);
                        });
            }

            long readEnd = System.currentTimeMillis();

            System.out.println("Read time: " + (readEnd - readStart) + " ms");
            bestReadTime = Math.min(bestReadTime, readEnd - readStart);
        }
        System.out.println("Best read time: " + bestReadTime + " ms");

        return (int) bestReadTime;
    }

    public static class OneBRCParquetRecord {
        public String city;
        public int temperature;

        public OneBRCParquetRecord() {}
        public OneBRCParquetRecord(String city, int temperature) {
            this.city = city;
            this.temperature = temperature;
        }

        public static MessageType schema = new MessageType(
                OneBRCParquetRecord.class.getSimpleName(),
                Types.required(BINARY).as(stringType()).named("city"),
                Types.required(INT32).named("temperature")
        );


        public void dehydrate(ValueWriter valueWriter) {
            valueWriter.write("city", city);
            valueWriter.write("temperature", temperature);
        }

        public OneBRCParquetRecord add(String heading, Object value) {
            if ("city".equals(heading)) {
                city = (String) value;
            } else if ("temperature".equals(heading)) {
                temperature = (Integer) value;
            } else {
                throw new UnsupportedOperationException("Unknown heading '" + heading + "'");
            }

            return this;
        }


    }

    static class OneBRCParquetHydrator implements Hydrator<OneBRCParquetRecord, OneBRCParquetRecord> {

        @Override
        public OneBRCParquetRecord start() {
            return new OneBRCParquetRecord();
        }

        @Override
        public OneBRCParquetRecord add(OneBRCParquetRecord target, String heading, Object value) {
            return target.add(heading, value);
        }

        @Override
        public OneBRCParquetRecord finish(OneBRCParquetRecord target) {
            return target;
        }

    }
}
