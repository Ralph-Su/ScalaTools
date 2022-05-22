package flink.datastream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCountExample {
  def main(args: Array[String]) {

    // 设置需要监听的本机地址与端口号
    val host: String = "127.0.0.1"
    val port: Int = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream(host, 9999)

    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()
    env.execute("Window Stream WordCount")
  }
}
