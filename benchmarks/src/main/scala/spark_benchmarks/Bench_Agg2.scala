package spark_benchmarks

import java.util.concurrent.TimeUnit
import org.apache.spark.sql.{ColumnName, DataFrame}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Bench_Agg2 extends BenchmarkParent {
  @Param(Array("100000", "1000000", "10000000", "100000000", "1000000000", "10000000000"))
  var sizes: Long = _

  var twoGroups: DataFrame = _

  @Setup(Level.Invocation)
  def setupTables() {
    twoGroups = sqlContext.range(0, sizes).select(new ColumnName("id") % 2 as 'a, (new ColumnName("id") as 'b))
    twoGroups.createOrReplaceTempView(s"twoGroups")
  }

  @TearDown(Level.Invocation)
  def teardownTables() {
    twoGroups.unpersist(true)
  }

  @Benchmark
  def twoGroupsAvg(bh: Blackhole) = {
    val avg = session.sql("SELECT AVG(b) FROM twoGroups GROUP BY a")
    sessionObj.touchDS(avg)
    bh.consume(avg)
  }

}