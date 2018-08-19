package spark_benchmarks

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
case class Data(id: Long)

@State(Scope.Benchmark)
case class SumAndCount(var sum: Long, var count: Int)

@State(Scope.Benchmark)
object TypedAverage extends Aggregator[Long, SumAndCount, Double] {
  override def zero: SumAndCount = SumAndCount(0L, 0)

  override def reduce(b: SumAndCount, a: Long): SumAndCount = {
    b.count += 1
    b.sum += a
    b
  }

  override def bufferEncoder = Encoders.product

  override def outputEncoder = Encoders.scalaDouble

  override def finish(reduction: SumAndCount): Double = reduction.sum.toDouble / reduction.count

  override def merge(b1: SumAndCount, b2: SumAndCount): SumAndCount = {
    b1.count += b2.count
    b1.sum += b2.sum
    b1
  }
}

@State(Scope.Benchmark)
object SparkSessionObj {

  var session = SparkSession.builder().master("local[*]").getOrCreate()
  session.sparkContext.setLogLevel("ERROR")
  var sqlContext = session.sqlContext
  var sparkContext = session.sparkContext

  def touchDS[A](ds: Dataset[A]) = ds.foreach(_ => Unit)

  def touchRDD[A](rdd: RDD[A]) = rdd.foreach(_ => Unit)

}

@State(Scope.Benchmark)
class SparkSessionC {

  var session = SparkSession.builder().master("local[*]").getOrCreate()
  session.sparkContext.setLogLevel("ERROR")
  var sqlContext = session.sqlContext
  var sparkContext = session.sparkContext


  def touchRDD[A](rdd: RDD[A]) = rdd.foreach(_ => Unit)

}

@State(Scope.Thread)
trait BenchmarkParent {

  var sessionObj = SparkSessionObj
  var session = sessionObj.session
  var sqlContext: SQLContext = sessionObj.sqlContext
  var sparkContext: SparkContext = sessionObj.sparkContext

}

trait BenchmarkWithTables extends BenchmarkParent {

  var oneMillionInts: DataFrame = _
  var oneHundredMillInts: DataFrame = _
  var oneBillionInts: DataFrame = _

  @Setup(Level.Invocation)
  def setupTables() {
    oneMillionInts = sqlContext.range(0, 1000000).repartition(1)
    oneMillionInts.createOrReplaceTempView("1milints")

    oneHundredMillInts = sqlContext.range(0, 100000000).repartition(10) // // 143.542mb, 10 files
    oneHundredMillInts.createOrReplaceTempView("100milints")

    oneBillionInts = sqlContext.range(0, 1000000000).repartition(10) // 1.4348gb, 10 files
    oneBillionInts.createOrReplaceTempView("1bilints")

  }

  @TearDown(Level.Invocation)
  def teardownTables() {
    oneMillionInts.unpersist(true)
    oneHundredMillInts.unpersist(true)
    oneBillionInts.unpersist(true)
  }

}