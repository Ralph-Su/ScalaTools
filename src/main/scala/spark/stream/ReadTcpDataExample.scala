package spark.stream

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object ReadTcpDataExample extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("ReadTcpDataExample")
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
   * */
  // 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
  val countDf = df.withColumn("words", functions.split(df("value"), " ")) // 把数组words展平为单word
    .withColumn("word", explode(col("words"))) // 以单词word为Key做分组
    .groupBy("word") // 分组计数
    .count()

  /**
   * 将Word Count结果写入到终端（Console）
   * Complete mode：输出到目前为止处理过的全部内容
   * Append mode：仅输出最近一次作业的计算结果
   * Update mode：仅输出内容有更新的计算结果
   * */
  countDf.writeStream
    .format("console") // 指定Sink为终端（Console）
    .option("truncate",  false) // 指定输出选项,“truncate”选项，用来表明输出内容是否需要截断。
    .outputMode("complete") //.outputMode("update")
    .start() // 启动流处理应用
    .awaitTermination() // 等待中断指令

}
