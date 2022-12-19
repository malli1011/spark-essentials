package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {
  val spark = SparkSession.builder().appName("Aggregations and Grouping").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //Counting
  moviesDF.select(count(col("Major_Genre"))) //all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  //count all and count distinct
  moviesDF.select(count("*")) // count all the rows and will INCLUDE nulls
  moviesDF.select(countDistinct(col("Major_Genre")).as("Distinct")).show(false)

  //min and max
  moviesDF.select(min(col("IMDB_Rating")).as("MinRating")).show(false)
  moviesDF.selectExpr("min(IMDB_Rating) as Min_Rating").show(false)

  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")
  moviesDF.select(avg("US_Gross"))

  //data science
  moviesDF.select(mean("Rotten_Tomatoes_Rating"), stddev("Rotten_Tomatoes_Rating")).show(false)

  //Grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")).count()
  private val avgRatingByGenreDF = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")
  avgRatingByGenreDF.show(false)

  moviesDF.groupBy("Major_Genre")
    .agg(
      count("*").as("Total_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(col("Avg_Rating"))
}



