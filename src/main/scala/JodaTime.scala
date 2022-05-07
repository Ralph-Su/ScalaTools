import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

object JodaTime extends App {
  val startStr = "2022-04-01"
  val endStr = "2022-04-30"
  // 设置要解析的字符串时间格式
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  // 解析字符串为DateTime
  val start:DateTime = formatter.parseDateTime(startStr)
  val end:DateTime = formatter.parseDateTime(endStr)
  // 计算开始日期和结束日期之间的天数
  val daysCount = Days.daysBetween(start, end).getDays()
  // 遍历开始日期和结束日期之间的每一天
  (0 until daysCount + 1).map(start.plusDays(_)).foreach(
    x=> {
      val curDate = x.toString("yyyy-MM-dd")
      val nextDate = x.plusDays(1).toString("yyyy-MM-dd")
      println(curDate, nextDate)
    }
  )

}
