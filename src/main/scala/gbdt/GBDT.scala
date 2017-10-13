/**
  * Created by cai on 10/13/17.
  */
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable.ListBuffer

/**
  * Created by cai on 9/11/17.
  */

object Predict{
  def labelData(data: Row, allLength: Int): LabeledPoint ={
    var value = new ListBuffer[Double]
    for(x <- 1 until allLength){
      val v = data(x)
      try {
        value.append(v.toString.toDouble)
      } catch {
        case e: NumberFormatException => value.append(1)
        case e: Exception => println(v)
      }
    }

    val list = value.toArray
    LabeledPoint(data(0).toString.toDouble, Vectors.dense(list.init))
    //    LabeledPoint(1.0, Vectors.dense(Array(1.0)))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic Regression")
    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val trainDf = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/home/cai/Desktop/kaggle/train_2016_v2_test.csv")

    val property = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/home/cai/Desktop/kaggle/properties_2016.csv")

    var train = trainDf.join(property, trainDf("parcelid") === property("parcelid"), "left")
    train = train.drop("parcelid")
    train = train.cache()
    //    val train = trainDf.join(property, Seq(trainDf("parcelid"), property("parcelid")), "left")
    train.show()
    import spark.implicits._
    val raw_size = train.columns.length
    // 不能直接把train.columns当参数传进去。
    val data = train.select("*").limit(100).na.fill("0").map{x =>labelData(x, raw_size)}.rdd
    for(x <- data.collect())
      println(x)


    val splits = data.randomSplit(Array(0.8, 0.2), seed = 123)
    val (trainingData, testData) = (splits(0), splits(1))

    // Setting up the hyper-parameters
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(50) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setNumClasses(2)
    boostingStrategy.treeStrategy.setMaxDepth(5)
    //    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

    // Run training algorithm to build the model
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Compute raw scores on the test set.
    val predictionAndLabels = testData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
  }
}
