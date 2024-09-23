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