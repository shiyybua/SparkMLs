import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression


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
    val training = phoneRankProcess.readCSVToDF(sess, "/home/cai/Desktop/phone_rank.csv", 0, ",").cache()
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficientMatrix} Intercept: ${lrModel.interceptVector}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

  }
}