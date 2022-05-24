package spark.sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
/**
  * @Author: suwenjin
  * @Description: filter算子的使用
  * @Time: 2022/5/24 11:47 AM
**/
object DataframeFilterExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")

  def readCsv(csvPath: String): DataFrame = {
    spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(csvPath)
  }

  val df1 = readCsv("src/main/resources/textfile_1.csv")
  val df2 = readCsv("src/main/resources/textfile_2.csv")

  /**
   * DataFrame filter() with Column condition
   */
  // This import is needed to use the $-notation and '-notation
  import spark.implicits._

  def useColumnCondition(): Unit = {
    df1.filter(col("name") === "suwenjin").show()
    df1.filter(df1("name") === "suwenjin").show()
    df1.filter($"name" === "suwenjin").show()
    df1.filter('name === "suwenjin").show()
    df1.filter('name =!= "suwenjin").show() // not equal
  }


  /**
   * DataFrame filter() with SQL Expression
   */
  def useSQLExpression(): Unit = {
    df1.filter("name == 'suwenjin'").show()
    df1.filter("name = 'suwenjin'").show()
  }

  /**
   * DataFrame filter() Multiple Conditions
   */
  def useMultipleConditions(): Unit = {
    df1.filter($"name" === "suwenjin" && $"age" === 12).show()
    df1.filter($"name" === "suwenjin" || $"age" =!= 12).show()
  }

}
