package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Bench_Duckdb_JDBC_Ordinal extends Benchmark {
    public Bench_Duckdb_JDBC_Ordinal() throws IOException {
        super(BenchmarkParameters.itemCount);;
    }

    @Override
    protected int runBenchmark(int iters) throws IOException {

        Path tempDir = Files.createTempDirectory(tempDirBase, "1brc-demo");

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try (var conn = DriverManager.getConnection("jdbc:duckdb:"+tempDir.resolve("data.db"));
             var createStatement = conn.createStatement()) {

            Path csvFile = tempDir.resolve("data.csv");

            long writeStart = System.currentTimeMillis();

            try (var bw = Files.newBufferedWriter(csvFile)) {
                for (int i = 0; i < itemCount; i++) {
                    var sample = nextMeasurement();
                    bw.write(sample.cityOrd() + "," + sample.shortValue() + "\n");
                }
            }

            System.out.println("Wrote CSV");

            // write to parquet
            createStatement.execute("PRAGMA memory_limit = '6GB';");
            createStatement.execute("SET preserve_insertion_order=false");
            createStatement.execute("CREATE TABLE measurements(cityOrd INT, temperature INT)");
            createStatement.execute("COPY measurements FROM '%s'".formatted(csvFile));

            long writeEnd = System.currentTimeMillis();
            System.out.println("Write time: " + (writeEnd - writeStart) + " ms");

            int bestReadTime = Integer.MAX_VALUE;


            for (int i = 0; i < iters; i++) {
                GcFinalization.awaitFullGc();

                ResultsObserver[] stats = new ResultsObserver[citiesAll.size()];
                Arrays.setAll(stats, j -> new ResultsObserver());

                long readStart = System.currentTimeMillis();
                try (var queryStmt = conn.createStatement();
                     var rs = queryStmt.executeQuery("SELECT cityOrd, temperature FROM measurements"))
                {
                    queryStmt.setFetchSize(10_000);
                    while (rs.next()) {
                        stats[rs.getInt(1)].observe(rs.getInt(2) / 100.);
                    }
                }

                for (int j = 0; j < stats.length; j++) {
                    System.out.println(citiesAll.get(j) + " " + stats[j]);
                }

                long readEnd = System.currentTimeMillis();
                System.out.println("Read time: " + (readEnd - readStart) + " ms");

                bestReadTime = (int) Math.min(bestReadTime, readEnd - readStart);
            }

            return bestReadTime;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // cleanup, we don't want to skew the output size
            Files.deleteIfExists(tempDir.resolve("data.csv"));
        }
    }

}
