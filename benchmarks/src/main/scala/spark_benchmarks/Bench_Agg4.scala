package spark_benchmarks

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Bench_Agg4 extends BenchmarkWithTables {

  @Param(Array("1milints", "100milints", "1bilints"))
  var table: String = _

  @Param(Array("SUM", "AVG", "COUNT", "STDDEV"))
  var agg: String = _


  @Benchmark
  def twoGroupsAvg(bh: Blackhole) = {
    val aggregated = session.sql(s"SELECT $agg(id) FROM $table")
    sessionObj.touchDS(aggregated)
    bh.consume(aggregated)
  }

}
