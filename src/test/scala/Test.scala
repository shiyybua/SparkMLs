import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by cai on 10/13/17.
  */
object Test{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("RandomForestRegressorExample")
      .getOrCreate()

    val data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("data/phone_rank.csv")
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
  }
}
