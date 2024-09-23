package benchmark;

import com.google.common.testing.GcFinalization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

public abstract class Benchmark {
    protected static List<String> citiesAll = List.of("helsinki", "tampere", "turku", "oulu", "vaasa", "pori", "jyväskylä", "mikkeli", "kouvola", "kotka",
            "stockholm", "gothenburg", "malmö", "uppsala", "västerås", "örebro", "linköping", "helsingborg", "jönköping", "norrköping",
            "copenhagen", "aarhus", "odense", "aalborg", "esbjerg", "randers", "kolding", "horsens", "vejle", "roskilde",
            "berlin", "hamburg", "munich", "cologne", "frankfurt", "stuttgart", "düsseldorf", "dortmund", "essen", "leipzig",
            "paris", "marseille", "lyon", "toulouse", "nice", "nantes", "strasbourg", "montpellier", "bordeaux", "lille",
            "london", "birmingham", "manchester", "glasgow", "liverpool", "newcastle", "sheffield", "leeds", "bristol", "nottingham",
            "madrid", "barcelona", "valencia", "seville", "zaragoza", "málaga", "murcia", "palma", "bilbao", "alicante",
            "rome", "milan", "naples", "turin", "palermo", "genoa", "bologna", "florence", "bari", "catania",
            "vienna", "graz", "linz", "salzburg", "innsbruck", "klagenfurt", "villach", "wels", "sankt pölten", "dornbirn",
            "prague", "brno", "ostrava", "plzeň", "liberec", "pardubice", "havirov", "zlin", "kromeriz", "karvina",
            "budapest", "debrecen", "szeged", "miskolc", "pecs", "gyor", "nyiregyhaza", "kecskemet", "szekesfehervar", "szombathely");
    double[] avgs;
    double[] stddevs;
    private final Random random = new Random();

    public static Path tempDirBase = Path.of("/tmp"); // may want to change this to a different directory if you're on tmpfs /tmp

    protected final int itemCount;
    protected final Path tempDir;

    public Benchmark(int itemCount) throws IOException {
        this.itemCount = itemCount;
        this.tempDir = Files.createTempDirectory(tempDirBase, getClass().getSimpleName());

        avgs = new double[citiesAll.size()];
        stddevs = new double[citiesAll.size()];


        for (int i = 0; i < avgs.length; i++) {
            avgs[i] = random.nextDouble(10, 30);
            stddevs[i] = random.nextDouble(0.1, 1.5);
        }
    }

    public record Measurement(int cityOrd, double value) {
        public String city() {
            return citiesAll.get(cityOrd);
        }

        public short shortValue() {
            return (short) (value * 100);
        }
    }

    public record BenchmarkResult(double timeS, double sizeGb) {
    }

    protected Measurement nextMeasurement() {
        int ord = random.nextInt(0, citiesAll.size());
        double measurement = random.nextGaussian(avgs[ord], stddevs[ord]);
        return new Measurement(ord, measurement);
    }

    abstract protected int runBenchmark(int iters) throws IOException;

    public BenchmarkResult run(int iters) throws IOException {
        try {
            int time = runBenchmark(iters);
            long sizeBytes = Files.list(tempDir).mapToLong(path -> {
                try {
                    long size = Files.size(path);
                    Files.delete(path);
                    return size;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return 0;
            }).sum();

            double normalizeFactor = 1_000_000_000 / (double) itemCount;
            return new BenchmarkResult(
                    Math.round(normalizeFactor * time / 10.) / 100.,
                    Math.round(10 * (normalizeFactor * sizeBytes) / 1024 / 1024 / 1024.)/10.);
        } catch (IOException ex) {
            Files.list(tempDir).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            throw ex;
        }
    }

    public String getName() {
        return getClass().getSimpleName();
    }
}
