package spark_benchmarks

import org.openjdk.jmh.runner.{Runner}
import org.openjdk.jmh.runner.options.{CommandLineOptions, OptionsBuilder}

object MyRunnerScala {

  val test2Run = "StreamsJ"

  def launchBenchmark(args: CommandLineOptions) {

    println("HOHO")

    val b = new OptionsBuilder()
      .parent(args)
      .param("scalaVersion", System.getProperty("scalaVersion"))
      //      .forks(1)
      //      .jvmArgsAppend("-Xmx4096m")
      .jvmArgsAppend("-Xmx2048m")


    b.include(test2Run)

    val res = new Runner(b.build()).run()
    println("**********************************")
    println(res.toString)

  }

}
