package scala_benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Simple_Bench {

  def increment(xs: Array[Int]) = xs.map(_ + 1)

  def filterOdd(xs: Array[Int]) = xs.filter(_ % 2 == 0)

  def sum(xs: Array[Int]) = xs.foldLeft(0L)(_ + _)

  @Benchmark
  def measure(bh: Blackhole) = {
    val numbers = (0 until 10000000).toArray
    val incrementedNumbers = increment(numbers)
    val evenNumbers = filterOdd(incrementedNumbers)
    val summed = sum(evenNumbers)
    bh.consume(summed)
  }
}