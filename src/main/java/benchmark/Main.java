package benchmark;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String... args) throws Exception {
        List<Benchmark> benchmarks = List.of(
                new Bench_Duckdb_JDBC_Ordinal(),
                new Bench_Duckdb_JDBC_String(),
                new Bench_Fury_String(),
                new Bench_Fury_Ordinal(),
                new Bench_CustomFileChannel_Strings4k(),
                new Bench_CustomFileChannel_Ordinal4k(),
                new Bench_CustomFileChannel_Strings32k(),
                new Bench_CustomFileChannel_Ordinal32k(),
                new Bench_ObjectInputStream(),
                new Bench_DataInputStream_Columnar(),
                new Bench_DataInputStream_Interleaved(),
                new Bench_DataInputStream_Interleaved_128k(),
                new Bench_DataInputStream_Interleaved_2m(),
                new Bench_DataInputStream_Interleaved_32m(),
                new Bench_Parquet(),
                new Bench_Protobuf_InputStream_String(),
                new Bench_Protobuf_InputStream_Ordinal(),
                new Bench_Protobuf_NIO_String(),
                new Bench_Protobuf_NIO_Ordinal(),
                new Bench_Protobuf_NIO_Ordinal32k(), // hn comment asserted 4k is too small a buffer
                new Bench_RAM_CityOrdinals(),
                new Bench_RAM_CityStrings()
        );

        Map<String, Benchmark.BenchmarkResult> results = new HashMap<>();

        for (var benchmark : benchmarks) {
            System.err.println("Running " + benchmark.getName());

            results.put(benchmark.getName(), benchmark.run(BenchmarkParameters.iters));
        }

        System.out.println("*** Results ***");

        results.entrySet()
                .stream()
                .sorted(Comparator.comparing(e -> e.getValue().timeS()))
                .forEach(entry -> {
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                });
    }
}
