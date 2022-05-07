import org.joda.time.Days
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTime

/**
 * @Author: suwenjin
 * @Description: Joda-Time 使用方法
 * @Time: 2022/5/7 8:32 PM
 * */

object JodaTime extends App {
  /**
   * @Description: 遍历UTC时间的某一天的24个小时
   * @Param localDate: 当地日期 eg: 2022-01-01
   * @Param format: 当地日期格式 eg: yyyy-MM-dd
   * @Param tz: 时区 eg: +8
   * @return: void
   * */
  def loopUTCHourOfDay(localDate: String, format: String, tz: Int): Unit = {
    // 设置要解析的字符串时间格式
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(format)
    // 解析字符串为DateTime
    val localTime: DateTime = formatter.parseDateTime(localDate)
    // 将当地时间(北京时间)转换为UTC时间
    val utcStartHour: DateTime = localTime.minusHours(tz)
    // 遍历24小时
    (0 to 23).map(utcStartHour.plusHours(_)).foreach(x => println(x.toString("yyyy-MM-dd'T'HH")))
  }

  /**
   * @Description: 根据开始日期和结束日期遍历每一天，开始日期和结束日期的格式可以自定义
   * @Param startDate: 开始日期 eg: 2022-01-01
   * @Param endDate: 结束日期 eg: 2022-01-30
   * @Param format: 时间格式 eg:yyyy-MM-dd
   * @return: void
   * */

  def loopDatePeriod(startDate: String, endDate: String, format: String): Unit = {
    // 设置要解析的字符串时间格式
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(format)
    // 解析字符串为DateTime
    val start: DateTime = formatter.parseDateTime(startDate)
    val end: DateTime = formatter.parseDateTime(endDate)
    // 计算开始日期和结束日期之间的天数
    val daysCount = Days.daysBetween(start, end).getDays()
    // 遍历开始日期和结束日期之间的每一天
    (0 to daysCount)
      .map(start.plusDays(_))
      .foreach(
        x => {
          val curDate = x.toString("yyyy-MM-dd")
          val nextDate = x.plusDays(1).toString("yyyy-MM-dd")
          println(curDate, nextDate)
        }
      )
  }

}
