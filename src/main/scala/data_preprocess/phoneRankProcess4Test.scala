import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._


/**
  * Created by cai on 10/13/17.
  */

object phoneRankProcess4Test{

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


  def modifyColumn(df: DataFrame): Unit ={
    val v = "csy"
    val fun:((String,Int,Int) => String) = (args:String, k1:Int, k2:Int) => { v }
    val sqlfunc = udf(fun)
    df.withColumn("f", sqlfunc(col("e"), lit(1), lit(3))).show()
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


  // for test
  def main(args: Array[String]): Unit = {
    val needPreprocessing = false
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (needPreprocessing) {
      val path = "data/phone_rank.csv"
      val sess = SparkSession
        .builder().master("local[*]")
        .appName("App")
        .getOrCreate()
      import sess.implicits._
      val training = sess.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load(path)
      //    val training = phoneRankProcess.readCSVToDF(sess, "data/test", 0, ",").cache()
      training.createOrReplaceTempView("test")
      training.show()
      //    fillColumnsWithAvg(sess, training, Seq("b","c","d"), "test").show()
      val newDF = categoryToDigit(sess, training, 100, "test")._1
      val table = categoryToDigit(sess, training, 100, "test")._2

      newDF.show()
      showReversedTable(table)
      try {
        newDF.write.format("com.databricks.spark.csv").option("header", "true").save("data/test.csv")

        val headerPath = "data/rank_header.txt"
        saveReversedTable(headerPath, table)
      } catch {
        case e: AnalysisException => println("file already exists")
      }
    } else {
      val spark = SparkSession
        .builder().master("local[*]")
        .appName("App")
        .getOrCreate()

      // $example on$
      // Load and parse the data file, converting it to a DataFrame.
//      val training = spark.read
//        .format("com.databricks.spark.csv")
//        .option("header", "true") //reading the headers
//        .option("mode", "DROPMALFORMED")
//        .load("data/test.csv")

      val data = readCSVToDF(spark,"data/test.csv", -1, ",")

      /*
      * +-----+--------------------+
        |label|            features|
        +-----+--------------------+
        |  0.0|(11,[0,1,2,4,5,6,...|    11是指总column数，也是最大的column index + 1
        |  1.0|(11,[0,1,3,4,7,10...|
      * */

      // Index labels, adding metadata to the label column.
      // Fit on whole dataset to include all labels in index.
      val labelIndexer = new StringIndexer()
        .setInputCol("label")             //这里的label必须要和上边的libsvm的ｈｅａｄｅｒ中label一致
        .setOutputCol("indexedLabel")
        .fit(data)
      // Automatically identify categorical features, and index them.
      // Set maxCategories so features with > 4 distinct values are treated as continuous.

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")          //这里的features必须要和上边的libsvm的ｈｅａｄｅｒ中features一致
        .setOutputCol("indexedFeatures")
        .setMaxCategories(100)  // 666666
        .fit(data)

      // labelIndexer, featureIndexer相当于给column重命名
      // Split the data into training and test sets (30% held out for testing).
      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      // Train a RandomForest model.
      val rf = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setNumTrees(10).setMaxBins(100)

      // Convert indexed labels back to original labels.
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      // Chain indexers and forest in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

      // Train model. This also runs the indexers.
      val model = pipeline.fit(trainingData)

      // Make predictions.
      val predictions = model.transform(testData)

      // Select example rows to display.
      predictions.select("predictedLabel", "label", "features").show(5)

      // Select (prediction, true label) and compute test error.
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("Test Error = " + (1.0 - accuracy))

      // stages是pipline里的第三个
      val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
      println("Learned classification forest model:\n" + rfModel.toDebugString)
      // $example off$
      rfModel.treeWeights.foreach(println)
      println(rfModel.trees.apply(0).featureImportances)
      spark.stop()
    }

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