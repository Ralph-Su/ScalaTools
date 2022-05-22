package spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author: suwenjin
 * @Description: Sparksession Example
 * @Time: 2022/5/8 12:06 PM
 * */

object SparkSessionExample extends App {

  /**
   * Spark 2.0 introduced a new entry point called SparkSession that essentially replaced both SQLContext and HiveContext.
   * Additionally, it gives to developers immediate access to SparkContext.
   */

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkSessionExample")
    .getOrCreate()

  println("Frist SparkContext:")
  println("APP Name :" + spark.sparkContext.appName);
  println("Deploy Mode :" + spark.sparkContext.deployMode);
  println("Master :" + spark.sparkContext.master);
  println("Spark Version :" + spark.version)

  spark.stop()

  var spark2 = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkSessionExample")
    .getOrCreate()

  println("Second SparkContext:")
  println("APP Name :" + spark2.sparkContext.appName);
  println("Deploy Mode :" + spark2.sparkContext.deployMode);
  println("Master :" + spark2.sparkContext.master);
  println("Spark Version :" + spark2.version)

  spark2.stop()
}
