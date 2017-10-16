import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by cai on 10/13/17.
  */

object phoneRankProcess{
  def readCSVToDF(sess: SparkSession, path: String, labelIndex: Int,
                  spliter: String, isRegression: Boolean): Unit ={
    val line = sess.sparkContext.textFile(path)
    line.map{ x=>
      var row = x.trim.split(spliter)
      val label = row(labelIndex)
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

      val DataSetFeatures = new SparseVector(featureSize, indices.toArray, values.toArray)
    }
  }

  // for test
  def main(args: Array[String]): Unit = {
    val x = "aa ba ca da ea"
    var row = x.trim.split(" ")
    var features = row.toBuffer[String]
    features.remove(0)
    for(x<-features)
      println(x)

    for(i <- features.indices){
      println(i)
      val value = features(i).trim
//      println(value.getClass.getSimpleName)
    }
  }
}