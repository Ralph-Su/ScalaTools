package spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @Author: suwenjin
  * @Description: 用SparkSQL处理时间格式
  * @Time: 2022/5/18 11:43 AM
**/
object TimeFormatExample extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkSessionExample")
    .getOrCreate()
  spark.sparkContext.setLogLevel("Error")

  import spark.implicits._

  /**
   * @Description: 解析字符串格式的时间，并格式化时间
   * @Param timeStr: 原始格式的时间
   * @Param originalFormat: 原始格式
   * @Param targetFormat: 目标格式
   * @return: void
   * */
  def timeFormat(originalTime: String, originalFormat: String, targetFormat: String): Unit = {
    val df = Seq(originalTime).toDF("time")
    df.createOrReplaceTempView("tmp_date_table")

    // sql函数：from_unixtime(unix_timestamp("${originalTime}", "${originalFormat}"), "${targetFormat}")
    spark.sql(
      s"""
         |select time,
         |       from_unixtime(unix_timestamp(time, \"${originalFormat}\"), \"${targetFormat}\")
         |from tmp_date_table
         |""".stripMargin
    ).show()
  }

  timeFormat("2022-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm")
  timeFormat("2022/01/01 12:00", "yyyy/MM/dd HH:mm", "yyyy-MM-dd HH:mm:ss")


}
