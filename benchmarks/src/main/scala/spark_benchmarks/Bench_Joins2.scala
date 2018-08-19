package spark_benchmarks

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{ColumnName, DataFrame}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MINUTES)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Bench_Joins2 extends BenchmarkParent {

  @Param(Array("1", "128", "256", "512", "1024"))
  var dataSize: Int = _

  var intsWithData: DataFrame = _

  @Setup(Level.Invocation)
  def setupTables() {
    intsWithData = sqlContext.range(0, 100000000).repartition(10) // // 143.542mb, 10 files
      .select(new ColumnName("id"), org.apache.spark.sql.functions.typedLit("*" * dataSize).as(s"data$dataSize"))
  }

  @TearDown(Level.Invocation)
  def teardownTables() {
    intsWithData.unpersist(true)
  }


  @Benchmark
  def varyDataSizeJoin(bh: Blackhole) = {
    val joined = intsWithData.as("a").join(intsWithData.as("b"), (new ColumnName("a.id") === (new ColumnName("b.id"))))
    sessionObj.touchDS(joined)
    bh.consume(joined)
  }

}
