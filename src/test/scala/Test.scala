import JodaTime.{loopDatePeriod, loopUTCHourOfDay}

object Test extends App {

   loopUTCHourOfDay("2022-01-01", "yyyy-MM-dd", +8)

   loopDatePeriod("2022-01-01", "2022-01-10", "yyyy-MM-dd")
}
