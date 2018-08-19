package spark_benchmarks

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MINUTES)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class Bench_Joins3 extends BenchmarkParent {

  @Param(Array("StringType", "IntegerType", "LongType", "DoubleType"))
  var keyTypeString: String = _

  var oneHundredMillInts: DataFrame = _

  @Setup(Level.Invocation)
  def setupTables() {
    oneHundredMillInts = sqlContext.range(0, 100000000).repartition(10) // // 143.542mb, 10 files

    val keyType = keyTypeString match {
      case "StringType" => StringType
      case "IntegerType" => IntegerType
      case "LongType" => LongType
      case "DoubleType" => DoubleType
    }

    oneHundredMillInts.select(new ColumnName("id").cast(keyType).as("id"))
    oneHundredMillInts.createOrReplaceTempView("100milints")
  }

  @TearDown(Level.Invocation)
  def teardownTables() {
    oneHundredMillInts.unpersist(true)
  }

  @Benchmark
  def varyKeyTypeJoin(bh: Blackhole) = {
    val joined = oneHundredMillInts.as("a").join(oneHundredMillInts.as("b"), new ColumnName("a.id") === new ColumnName("b.id"))
    sessionObj.touchDS(joined)
    bh.consume(joined)
  }


}
