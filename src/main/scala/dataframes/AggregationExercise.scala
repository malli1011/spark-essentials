package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, countDistinct, sum}

object AggregationExercise extends App {
  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */

  val spark = SparkSession.builder().appName("Aggregations and Grouping").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //1
  moviesDF.withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
    .select(sum("Total_Gross")).show(false)

  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross")).show(false)

  //2
  moviesDF.select(countDistinct("Director")).show(false)

  moviesDF.groupBy("Director").agg(
    avg("IMDB_Rating").as("Avg_Rating"),
    sum("US_Gross").as("Total_US_Gross")
  ).orderBy(col("Avg_Rating").desc_nulls_last).show(false)

}
