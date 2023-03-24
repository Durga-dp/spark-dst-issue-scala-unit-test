import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object TestSparkDst {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val orignalDF = Seq(("Durga_DST", "2019-03-10 02:02:02"),
      ("Ion", "2019-03-13 02:01:10"),
      ("Robert_DST", "2023-03-12 02:20:59"),
      ("Sam", "2020-03-12 02:20:59")
    ).toDF("ename", "login_timestamp_raw")

    println("spark.version:" + spark.version)
    println("-------------------------------------original dataframe----------------------------------------------")
    orignalDF.show()
    orignalDF.printSchema()
    println("----------------------------------------------dataframe with cast----------------------------------------------")
    val transformedDF= orignalDF.select(
      col("ename"),
      col("login_timestamp_raw"),
      col("login_timestamp_raw")
        .cast(TimestampType).
        as("login_timestamp_converted"))
    transformedDF.printSchema()
    transformedDF  .show(false)
    println("----------------------------------------------dataframe-covert to unix_ts and then cast----------------------------------------------")
    val dfUnixTsCast = orignalDF.select(
      col("ename"),
      col("login_timestamp_raw"),
      unix_timestamp(orignalDF.col("login_timestamp_raw"), "yyyy-MM-dd hh:mm:ss").as("unix_timestamp_epoch"))
    dfUnixTsCast.printSchema()
    dfUnixTsCast.show(false)
    println("spark.version:" + spark.version)
  }
}