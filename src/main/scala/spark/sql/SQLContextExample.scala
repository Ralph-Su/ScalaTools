package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: suwenjin
  * @Description: SQLContext Example
  * @Time: 2022/5/8 12:12 PM
**/

object SQLContextExample extends App {
  /**
   * SQLContext is the entry point to SparkSQL which is a Spark module for structured data processing.
   * Once SQLContext is initialised, the user can then use it in order to perform various “sql-like” operations over Datasets and Dataframes.
   * https://spark.apache.org/docs/1.6.1/sql-programming-guide.html#starting-point-sqlcontext
   */
  val sparkConf = new SparkConf()
    .setAppName("SQLContextExample")
    .setMaster("local[1]")

  val sparkContext = new SparkContext(sparkConf)
  // sqlContext => DataFrame, before spark 2.0 you can do this, now you should use SparkSession
  val sqlContext = new SQLContext(sparkContext)

  // sparkContext => RDD
  val rdd : RDD[String] = sparkContext.textFile("src/main/resources/textfile_1.csv")
  // sqlContext => DataFrame
  val df : DataFrame = sqlContext.read.csv("src/main/resources/textfile_1.csv")
  df.printSchema()

  sparkContext.stop()

}
