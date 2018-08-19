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
class Bench_Joins1 extends BenchmarkWithTables {

  @Param(Array("JOIN", "RIGHT JOIN", "LEFT JOIN", "FULL OUTER JOIN"))
  var joinType: String = _

  @Param(Array("1milints", "100milints", "1bilints"))
  var table1: String = _

  @Param(Array("1milints", "100milints", "1bilints"))
  var table2: String = _

  @Benchmark
  def singleKeyJoin(bh: Blackhole) = {
    val count = session.sql(s"SELECT COUNT(*) FROM $table1 a $joinType $table2 b ON a.id = b.id")
    sessionObj.touchDS(count)
    bh.consume(count)
  }

}
