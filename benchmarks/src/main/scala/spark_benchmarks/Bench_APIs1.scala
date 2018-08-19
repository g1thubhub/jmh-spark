package spark_benchmarks

import java.util.concurrent.TimeUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class Bench_APIs1 extends BenchmarkParent {

  @Param(Array("1"))
  var start: Long = _
  @Param(Array("100000000"))
  var numLongs: Long = _


  @Benchmark
  def rangeRDD(bh: Blackhole) = {
    val rdd: RDD[Long] = sparkContext.range(start, numLongs)
    sessionObj.touchRDD(rdd)
    bh.consume(rdd)
  }

  @Benchmark
  def rangeDataset(bh: Blackhole) = {
    val ds: Dataset[Long] = session.range(start, numLongs).as[Long](Encoders.scalaLong)
    sessionObj.touchDS(ds)
    bh.consume(ds)
  }

  @Benchmark
  def rangeDatasetJ(bh: Blackhole) = {
    val ds: Dataset[java.lang.Long] = session.range(start, numLongs)
    sessionObj.touchDS(ds)
    bh.consume(ds)
  }

  @Benchmark
  def rangeDataframe(bh: Blackhole) = {
    val df: DataFrame = sqlContext.range(start, numLongs)
    sessionObj.touchDS(df)
    bh.consume(df)
  }

}
