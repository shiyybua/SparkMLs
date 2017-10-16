import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors


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
      labelIndex = schemaString.length + labelIndex - 1
    val rows = line.filter(_ != header).map{ x=>
      var row = x.trim.split(spliter)
      val label = row(labelIndex).toDouble
      val featureSize = row.length-1
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
//      val DataSetFeatures = new SparseVector(featureSize, indices.toArray, values.toArray)
//      Row(label, DataSetFeatures)
      Row(label, Vectors.sparse(featureSize, indices.toArray, values.toArray))
    }
    val schema = StructType(Array(StructField("label",DoubleType,true), StructField("features",VectorType,true)))
//    val schema = StructType(schemaString.map(fieldName=>StructField(fieldName,StringType,true)))
    sess.sqlContext.createDataFrame(rows, schema)
  }

  // for test
  def main(args: Array[String]): Unit = {
    val sess = SparkSession
      .builder().master("local[*]")
      .appName("App")
      .getOrCreate()
    val df = phoneRankProcess.readCSVToDF(sess, "/home/cai/Desktop/phone_rank.csv", -1, ",")
    df.show()

  }
}