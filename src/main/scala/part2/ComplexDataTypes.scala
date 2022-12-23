package part2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataTypes extends App {
  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App")
    .master("local[1]")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.where(col("US_DVD_Sales").isNotNull)
  //.show(false)

  //working with dates

  val moviesWithDatesDF = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) //converts string to date
    .withColumn("Today", current_date()) //today's date
    .withColumn("Right_now", current_timestamp()) //current timestamp
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")))
  //.show(false)

  val stocksDF = spark.read.option("inferSchema", "true").option("header", "true").csv("src/main/resources/data/stocks.csv")
  stocksDF.withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
  //.show(false)

  //Structures

  //1 - with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
  //.show(false)

  //2 - with expression strings

  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
  //.show(false)

  //3 - Arrays
  val movesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_words")) //Array of string

  movesWithWords.select(
    col("Title"),
    expr("Title_words[0]"),
    size(col("Title_words")),
    array_contains(col("Title_words"), "Love").as("IsLoveStory")
  ).filter(col("IsLoveStory"))
    .show(100, false)

}
