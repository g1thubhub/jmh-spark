package spark_benchmarks

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{ColumnName, DataFrame}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MINUTES)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Bench_Joins4 extends BenchmarkParent {

  @Param(Array("1", "2", "4", "8", "16"))
  var numCopies: Int = _

  var oneHundredMillInts: DataFrame = _
  var copiedInts: DataFrame = _


  @Setup(Level.Invocation)
  def setupTables() {
    oneHundredMillInts = sqlContext.range(0, 100000000).repartition(10) // // 143.542mb, 10 files
    copiedInts = Seq.fill(numCopies)(oneHundredMillInts).reduce(_ union _)
  }

  @TearDown(Level.Invocation)
  def teardownTables() {
    oneHundredMillInts.unpersist(true)
    copiedInts.unpersist(true)
  }

  @Benchmark
  def varyNumTMatchesJoin(bh: Blackhole) = {

    val joined = copiedInts.as("a")
      .join(oneHundredMillInts.as("b"), new ColumnName("a.id") === new ColumnName("b.id"))
    sessionObj.touchDS(joined)
    bh.consume(joined)

  }


}
