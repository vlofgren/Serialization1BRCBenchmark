Benchmark code to go with the blog post on the same topic. 

https://www.marginalia.nu/log/a_110_java_io/

Some of the code is not identical to the code in the blog post, but homomorphic and
the results are the same. 

You can configure the number of items and iterations in the `BenchmarkParamseters` class,
for developing new benchmarks 10,000,000 is a more tolerable number of items ;-)

The code assumes Java 22.

Gotchas: 

* Be sure to set your CPU governor to performance mode
* Write to a device that is not used for anything else 
* You ideally want a fairly significant amount of RAM for the 1B test, ideally 32 GB or more, 
  otherwise try running a smaller set.

Here is a reference output:

BenchRAM_CityOrdinals: BenchmarkResult[timeS=2.73, sizeGb=0.0]
Bench_CustomFileChannel_Ordinal: BenchmarkResult[timeS=6.91, sizeGb=2.8]
BenchRAM_CityStrings: BenchmarkResult[timeS=12.34, sizeGb=0.0]
Bench_Protobuf_NIO_Ordinal32k: BenchmarkResult[timeS=20.34, sizeGb=7.4]
Bench_Protobuf_NIO_Ordinal: BenchmarkResult[timeS=39.71, sizeGb=7.4]
Bench_CustomFileChannel_Strings: BenchmarkResult[timeS=58.99, sizeGb=9.6]
Bench_Protobuf_NIO_String: BenchmarkResult[timeS=88.48, sizeGb=12.4]
Bench_Parquet: BenchmarkResult[timeS=122.98, sizeGb=2.2]
Bench_DataInputStream_Interleaved: BenchmarkResult[timeS=128.89, sizeGb=10.5]
Bench_DataInputStream_Columnar: BenchmarkResult[timeS=130.92, sizeGb=10.5]
Bench_ObjectIntputStream: BenchmarkResult[timeS=276.69, sizeGb=14.0]
Bench_Protobuf_InputStream_Ordinal: BenchmarkResult[timeS=528.78, sizeGb=7.4]
Bench_Protobuf_InputStream_String: BenchmarkResult[timeS=565.53, sizeGb=12.4]
Bench_Fury_Ordinal: BenchmarkResult[timeS=772.35, sizeGb=4.1]
Bench_Fury_String: BenchmarkResult[timeS=787.56, sizeGb=11.4]
