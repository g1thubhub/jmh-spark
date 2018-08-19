package spark_benchmarks;

import org.openjdk.jmh.runner.options.CommandLineOptions;

public class MyRunner {
    public static void main(String[] args) throws Exception {
        System.out.println("####################################");
        CommandLineOptions opts = new CommandLineOptions(args);
        System.out.println(opts);
        MyRunnerScala.launchBenchmark(opts);
    }
}

