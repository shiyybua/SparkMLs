import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._


/**
  * Created by cai on 10/13/17.
  */

object phoneRankProcess{
  def readCSVToDF(sess: SparkSession, path: String, _labelIndex: Int,
                  spliter: String): DataFrame ={
    val line = sess.sparkContext.textFile(path)
    val header = line.first()
    val schemaString = header.trim.split(spliter)
    var labelIndex = _labelIndex
    if(labelIndex < 0)
      labelIndex = schemaString.length + labelIndex
    val featureSize = schemaString.length - 1
    val rows = line.filter(_ != header).map{ x=>
      var row = x.trim.split(spliter)
      val label = row(labelIndex).toDouble
      val features = row.toBuffer
      features.remove(labelIndex)
      val indices = ListBuffer[Int]()
      val values = ListBuffer[Double]()

      for(i <- features.indices){
        val value = features(i).trim
        if(value != ""){
          indices.append(i)
          values.append(value.toDouble)
        }
      }
      Row(label, Vectors.sparse(featureSize, indices.toArray, values.toArray))
    }
    val schema = StructType(Array(StructField("label",DoubleType,true), StructField("features",VectorType,true)))
    sess.sqlContext.createDataFrame(rows, schema)
  }

  def fillColumnsWithAvg(sess: SparkSession, df: DataFrame ,columns: Seq[String], tableName: String): DataFrame ={
    // update某列 ： val updatedDf = newDf.withColumn("c", regexp_replace(col("c"), "null", avg.toString))
    var valueMap = scala.Predef.Map[String, Any]()
    for(column <- columns){
      val avg = sess.sql(s"select avg($column) from $tableName where $column is not null").first().get(0)
      valueMap += (column -> avg)
    }
    df.na.fill(valueMap)
  }

  def categoryToDigit(sess: SparkSession, df: DataFrame, columns: Seq[String], tableName: String): Unit ={}

  def categoryToDigit(sess: SparkSession, df: DataFrame, maxCategory: Int, tableName: String): Unit ={
    for(x <- df.columns){
      // 返回该列所有不重复的数．
      val category = sess.sql(s"select $x from $tableName").distinct()
      if(category.count() <= maxCategory){
        val distinctVal = category.distinct().collect().map(_.get(0).toString)
        val indexedValue = scala.collection.mutable.Map[String, Int]()
        for(i <- distinctVal.indices)
          indexedValue += (distinctVal(i) -> i)

        df.select(x).map{row =>

        }
      }
    }
  }


  // for test
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val path = "/home/cai/Desktop/phone_rank.csv"
    val sess = SparkSession
      .builder().master("local[*]")
      .appName("App")
      .getOrCreate()
    val training = sess.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("data/test")
//    val training = phoneRankProcess.readCSVToDF(sess, "data/test", 0, ",").cache()
    training.createOrReplaceTempView("test")

    fillColumnsWithAvg(sess, training, Seq("b","c","d"), "test").show()
    categoryToDigit(sess, training, 5, "test")
  }
}
/*
* +---+----+----+----+
|  l|   b|   c|   d|
+---+----+----+----+
|  0|   2|null|   3|
|  1|   7|   5|   1|
|  0|   1|   2|null|
|  1|null|null|null|
+---+----+----+----+
* */