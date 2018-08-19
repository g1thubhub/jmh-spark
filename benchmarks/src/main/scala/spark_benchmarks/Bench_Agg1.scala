package spark_benchmarks

import java.util.concurrent.TimeUnit
import org.apache.spark.sql.DataFrame
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Bench_Agg1 extends BenchmarkParent {
  @Param(Array("10", "100", "1000", "10000", "100000", "1000000"))
  var sizes: Int = _

  var intsTable: DataFrame = _

  @Setup(Level.Invocation)
  def setupTables() {
    val rdd = sparkContext.parallelize(1 to sizes).flatMap { group =>
      (1 to 10000).map(i => (group, i))
    }
    intsTable = session.createDataFrame(rdd)
      .withColumnRenamed("_1", "a")
      .withColumnRenamed("_2", "b")
    intsTable.createOrReplaceTempView("intsTable")
  }

  @TearDown(Level.Invocation)
  def teardownTables() {
    intsTable.unpersist(true)
  }

  @Benchmark
  def varyNumGroupsAvg(bh: Blackhole) = {
    val grouped = session.sql("SELECT AVG(b) FROM intsTable GROUP BY a")
    sessionObj.touchDS(grouped)
    bh.consume(grouped)

  }
}
