package spark.stream

import org.apache.spark.sql.functions.{col, element_at, explode, split, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: suwenjin
 * @Description: Structured Streaming Word Count With Time Windows
 * @Time: 2022/5/21 11:15 AM
 * */
object WordCountWithWindowsExample extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("WordCountWithWindowsExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")

  // 设置需要监听的本机地址与端口号
  val host: String = "127.0.0.1"
  val port: String = "9999"

  //  nc -lk 9999

  // 从监听地址创建DataFrame
  var df: DataFrame = spark.readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load()

  /**
   * 使用DataFrame API完成Word Count计算
   * test data:
   * 2021-10-01 09:30:00, Apache Spark
   * 2021-10-01 09:34:00, Apache Logo
   * 2021-10-01 09:36:00, Structured Streaming
   * 2021-10-01 09:39:00, Spark Streaming
   * 2021-10-01 09:41:00, AMP Lab
   * 2021-10-01 09:44:00, Spark SQL
   * 2021-10-01 09:29:00, Test Test
   * 2021-10-01 09:33:00, Spark is cool
   *
   *分析：
   *   event time                               水印        水位线       上沿      下沿
2021-10-01 09:30:00, Apache Spark           09:30:00    09:20:00  09:25:00    09:20:00  ok
2021-10-01 09:36:00, Structured Streaming   09:36:00    09:26:00  09:30:00    09:25:00  ok
2021-10-01 09:39:00, Spark Streaming        09:39:00    09:29:00  09:30:00    09:25:00  ok
2021-10-01 09:41:00, AMP Lab                09:41:00    09:31:00  09:35:00    09:30:00  ok
2021-10-01 09:44:00, Spark SQL              09:44:00    09:34:00  09:35:00    09:30:00  ok
2021-10-01 09:29:00, Test Test              09:44:00    09:34:00  09:35:00    09:30:00  no
2021-10-01 09:33:00, Spark is cool          09:44:00    09:33:00  09:35:00    09:30:00  ok
   * */

  val countDf = df.withColumn("inputs", split(col("value"), ","))
    // 提取事件时间
    .withColumn("eventTime", element_at(col("inputs"),1).cast("timestamp"))
    // 提取单词序列
    .withColumn("words", split(element_at(col("inputs"),2), " "))
    // 展开单词序列
    .withColumn("word", explode(col("words")))
    // 启用Watermark机制，指定容忍度T为10分钟
    .withWatermark("eventTime", "10 minute")
    // 按照Tumbling Window与单词做分组
    .groupBy(window(col("eventTime"), "5 minute"), col("word"))
    // 统计个数
    .count()

  /**
   * 将Word Count结果写入到终端（Console）
   * Complete mode：输出到目前为止处理过的全部内容
   * Append mode：仅输出最近一次作业的计算结果
   * Update mode：仅输出内容有更新的计算结果
   * */
  countDf.writeStream
    .format("console") // 指定Sink为终端（Console）
    .option("truncate", false) // 指定输出选项,“truncate”选项，用来表明输出内容是否需要截断。
//    .outputMode("complete")
    .outputMode("update")
    .start() // 启动流处理应用
    .awaitTermination() // 等待中断指令

}
