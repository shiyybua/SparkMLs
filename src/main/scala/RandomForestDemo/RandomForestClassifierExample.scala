/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import data_preprocess.DataProcessing
import org.apache.spark.sql.AnalysisException
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }


object RandomForestClassifierExample {

  def train(processor: DataProcessing): Unit ={
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("App")
      .getOrCreate()

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")

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
      .setMaxCategories(4)  // 666666
      .fit(data)

    // labelIndexer, featureIndexer相当于给column重命名
    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

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

  def preprocess(processor: DataProcessing, inputPath: String, outputPath: String, headerPath: String): Unit ={
    val sess = SparkSession
      .builder().master("local[*]")
      .appName("App")
      .getOrCreate()
    import sess.implicits._
    val training = sess.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load(inputPath)
    training.createOrReplaceTempView("test")
    val newDF = processor.categoryToDigit(sess, training, 100, "test")._1
    val table = processor.categoryToDigit(sess, training, 100, "test")._2

    processor.showReversedTable(table)
    try {
      newDF.write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)

      processor.saveReversedTable(headerPath, table)
    } catch {
      case e: AnalysisException => println("file already exists")
    }
  }

  def main(args: Array[String]): Unit = {
    val operation = "train"
    val inputPath = "data/phone_rank.csv"
    val outputPath = "data/processed_data"
    val headerPath = "data/rank_header.txt"
    val preprocessor = new DataProcessing()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (operation == "train"){
      train(preprocessor)
    } else if (operation == "predict"){

    } else if (operation == "preprocess"){
      preprocess(preprocessor, inputPath, outputPath, headerPath)
    }

  }
}
// scalastyle:on println
