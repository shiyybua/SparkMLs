package data_preprocess

import java.io.FileWriter

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by cai on 10/19/17.
  */

class DataProcessing{
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

  def categoryToDigit(sess: SparkSession, df: DataFrame, maxCategory: Int, tableName: String):
              Tuple2[DataFrame, Map[String, scala.collection.mutable.Map[Int, String]]] ={
    var newDF = df
    var categoryTable = Map[String, scala.collection.mutable.Map[Int, String]]()

    for(x <- df.columns){
      // 返回该列所有不重复的数．
      val category = sess.sql(s"select $x from $tableName").distinct()
      if(category.count() <= maxCategory){
        val distinctVal = category.distinct().collect().filter(_.get(0) != null).map(_.get(0).toString)
        val indexedValue = scala.collection.mutable.Map[String, Int]()
        val reversedIndexedValue = scala.collection.mutable.Map[Int, String]()

        for(i <- distinctVal.indices) {
          indexedValue += (distinctVal(i) -> i)
          reversedIndexedValue += (i -> distinctVal(i))
        }

        val convertFunc: ((String) => Option[Int])
        = (column: String) => {
          indexedValue.get(column)
        }

        val sqlfunc = udf(convertFunc)
        newDF = newDF.withColumn(x, sqlfunc(col(x)))

        categoryTable += (x -> reversedIndexedValue)
      }

    }
    Tuple2(newDF, categoryTable)
  }


  def showReversedTable(reverseTable: Map[String, scala.collection.mutable.Map[Int, String]]): Unit ={
    for((columnName, table) <- reverseTable){
      println(columnName + ":")
      for((key, value) <- table){
        println(key + "－＞" + value)
      }
      println("*********************************************")
    }
  }

  def saveReversedTable(filePath: String, reversedTable: Map[String, scala.collection.mutable.Map[Int, String]]): Unit ={
    val out = new FileWriter(filePath, false)
    for((columnName, table) <- reversedTable){
      out.write(columnName+"\n")
      for((key, value) <- table){
        out.write(key + "\t" + value + "\n")
      }
    }
    out.close()
  }
}