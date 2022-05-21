package spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lower, sum, trim}
import org.apache.spark.storage.StorageLevel

object DataAnalysisExample extends App {
  var spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("DataAnalysisExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  def transformColumns(csvPath: String): DataFrame = {
    val df: DataFrame = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(csvPath)

    val AnalysisDf = df
      .select(
        trim(lower(col("订阅名称 (Subscription Name)"))).as("Country"),
        trim(lower(col("日期 (Date)"))).as("Date"),
        trim(lower(col("服务类型 (Meter Sub-Category)"))).as("SubCategory"),
        trim(lower(col("服务 (Meter Category)"))).as("Category"),
        trim(lower(col("服务资源 (Meter Name)"))).as("MeterName"),
        col("扩展的成本 (ExtendedCost)").as("ExtendedCost"),
        trim(lower(col("(Resource Group)"))).as("ResourceGroup")
      )
    AnalysisDf
  }

  def compare(): Unit = {
    val costDetails202205 = transformColumns("src/main/resources/2022_5-cn.csv").persist(StorageLevel.MEMORY_ONLY)
    val costDetails202203 = transformColumns("src/main/resources/2022_3-cn.csv")
    val costDetails202204 = transformColumns("src/main/resources/2022_4-cn.csv")

    def semiAndSum(df : DataFrame): DataFrame = {
       df.as("t1")
        .join(costDetails202205.as("t2"),
          col("t1.ResourceGroup") === col("t2.ResourceGroup")
            && col("t1.Category") === col("t2.Category"),
          "leftsemi")
        .groupBy(col("t1.ResourceGroup"), col("t1.Category"), col("t1.MeterName"))
        .agg(sum("ExtendedCost").as("ExtendedCost"))
    }

    val costSum202203 = semiAndSum(costDetails202203).sort(col("ExtendedCost").desc)
    val costSum202204 = semiAndSum(costDetails202204).sort(col("ExtendedCost").desc)

    costSum202203.show(false)
    costSum202204.show(false)

    val compareDetails = costSum202203.as("t1")
      .join(
        costSum202204.as("t2"),
        col("t1.ResourceGroup") === col("t2.ResourceGroup")
          && col("t1.Category") === col("t2.Category")
          && col("t1.MeterName") === col("t2.MeterName"),
        "inner"
      ).select(col("t1.ResourceGroup"),
      col("t1.Category"),
      col("t1.MeterName"),
      col("t1.ExtendedCost").as("202203cost"),
      col("t2.ExtendedCost").as("202204cost"),
      (col("t2.ExtendedCost") - col("t1.ExtendedCost")).as("diff")
    )

    compareDetails
      .filter(col("diff") > 0)
      .sort(col("ResourceGroup"),col("Category"))
      .show(100,truncate = false)

    compareDetails.groupBy(col("ResourceGroup"))
      .agg(sum("diff").as("diffSum"))
      .sort(col("diffSum").desc)
      .show(100, truncate = false)
  }

  compare()
}
