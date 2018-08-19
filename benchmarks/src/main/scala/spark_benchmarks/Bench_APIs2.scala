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
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class Bench_APIs2 extends BenchmarkParent {

  @Param(Array("1"))
  var start: Long = _
  @Param(Array("1000000"))
  var smallNumLongs: Long = _
  @Param(Array("100000000"))
  var numLongs: Long = _

  var ds: Dataset[Data] = _
  var df: DataFrame = _
  var rdd: RDD[Data] = _
  var smallDf: DataFrame = _
  var smallDs: Dataset[Long] = _
  var smallRdd: RDD[Long] = _

  @Setup(Level.Invocation)
  def setup() {
    ds = session.range(start, numLongs).as[Data](Encoders.product)
    df = sqlContext.range(start, numLongs)
    rdd = sparkContext.range(1L, numLongs).map(Data(_))
    smallDf = sqlContext.range(start, smallNumLongs)
    smallDs = sqlContext.range(1, smallNumLongs).as[Long](Encoders.scalaLong)
    smallRdd = sparkContext.range(1, smallNumLongs)
  }

  @TearDown(Level.Invocation)
  def teardown() {
    ds.unpersist(true)
    df.unpersist(true)
    rdd.unpersist(true)
    smallDf.unpersist(true)
    smallRdd.unpersist(true)
    smallDs.unpersist(true)
  }


  // Actual benchmarks
  @Benchmark
  def filterRDD(bh: Blackhole) = {
    val filteredRdd: RDD[Data] = rdd
      .filter(_.id % 100 != 0)
      .filter(_.id % 101 != 0)
      .filter(_.id % 102 != 0)
      .filter(_.id % 103 != 0)
    sessionObj.touchRDD(filteredRdd)
    bh.consume(filteredRdd)
  }

  @Benchmark
  def filterDataframe(bh: Blackhole) = {
    val filteredDf: DataFrame = df
      .filter("id % 100 != 0")
      .filter("id % 101 != 0")
      .filter("id % 102 != 0")
      .filter("id % 103 != 0")
    sessionObj.touchDS(filteredDf)
    bh.consume(filteredDf)
  }

  @Benchmark
  def filterDataset(bh: Blackhole) = {
    val filteredDS: Dataset[Data] = ds
      .filter(_.id % 100 != 0)
      .filter(_.id % 101 != 0)
      .filter(_.id % 102 != 0)
      .filter(_.id % 103 != 0)
    sessionObj.touchDS(filteredDS)
    bh.consume(filteredDS)
  }

  @Benchmark
  def mapRDD(bh: Blackhole) = {
    val mappedRdd: RDD[Data] = rdd
      .map(d => Data(d.id + 1L))
      .map(d => Data(d.id + 1L))
      .map(d => Data(d.id + 1L))
      .map(d => Data(d.id + 1L))
    sessionObj.touchRDD(mappedRdd)
    bh.consume(mappedRdd)
  }

  @Benchmark
  def mapDataframe(bh: Blackhole) = {
    val mappedDf: DataFrame = df
      .select(new ColumnName("id") + 1 as 'id)
      .select(new ColumnName("id") + 1 as 'id)
      .select(new ColumnName("id") + 1 as 'id)
      .select(new ColumnName("id") + 1 as 'id)
    sessionObj.touchDS(mappedDf)
    bh.consume(mappedDf)
  }

  @Benchmark
  def mapDataset(bh: Blackhole) = {
    val mappedDS: Dataset[Data] = ds
      .map(d => Data(d.id + 1L))(Encoders.product)
      .map(d => Data(d.id + 1L))(Encoders.product)
      .map(d => Data(d.id + 1L))(Encoders.product)
      .map(d => Data(d.id + 1L))(Encoders.product)
    sessionObj.touchDS(mappedDS)
    bh.consume(mappedDS)
  }

  @Benchmark
  def averageRDD(bh: Blackhole) = {
    val sumAndCount = smallRdd
      .map(i => (i, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val avg = sumAndCount._1.toDouble / sumAndCount._2
    bh.consume(avg)
  }

  @Benchmark
  def averageDataframe(bh: Blackhole) = {
    val avg: DataFrame = smallDf.selectExpr("avg(id)")
    bh.consume(avg.collect())
  }

  @Benchmark
  def averageDataset(bh: Blackhole) = {
    val d = smallDs.select(TypedAverage.toColumn)
    bh.consume(d.collect())
  }

}
