import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

/**
  * Created by cai on 10/19/17.
  */

object DataProcessing{
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

  def fillColumnsWithAvg(sess: SparkSession, df: DataFrame ,columns: Seq[String], tableName: String): DataFrame ={
    // update某列 ： val updatedDf = newDf.withColumn("c", regexp_replace(col("c"), "null", avg.toString))
    var valueMap = scala.Predef.Map[String, Any]()
    for(column <- columns){
      val avg = sess.sql(s"select avg($column) from $tableName where $column is not null").first().get(0)
      valueMap += (column -> avg)
    }
    df.na.fill(valueMap)
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
}