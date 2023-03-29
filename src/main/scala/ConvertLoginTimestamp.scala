import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ConvertLoginTimestamp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val inputPath="src/test/resources/login_dates.csv"
    val originalDF=readDataFrame(spark,inputPath)
    val dfUnixTsCast = covertStringToTimestampType(originalDF)
  }
  def readDataFrame(spark:SparkSession,inputPath:String):DataFrame={
    spark.read.format("csv").option("header","true").load(inputPath)
  }
  def covertStringToTimestampType(df:DataFrame):DataFrame={
    df.select(
      col("login_timestamp_raw"),
      unix_timestamp(df.col("login_timestamp_raw"), "yyyy-MM-dd hh:mm:ss").as("unix_timestamp_epoch"))
  }
}