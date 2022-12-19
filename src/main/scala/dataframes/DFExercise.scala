package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types.StructType

object DFExercise extends App {
  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  val spark = SparkSession.builder().appName("DF Exercise").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //1
  moviesDF.select("Title", "IMDB_Rating").show(false)
  moviesDF.selectExpr("Title", "IMDB_Rating").show(false)

  //2
  moviesDF.select(col("Title"), col("US_Gross"), col("Worldwide_Gross")).withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross")).show(false)
  moviesDF.select(col("Title"), col("US_Gross"), col("Worldwide_Gross"), (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")).show()
  moviesDF.selectExpr("Title", "US_Gross", "Worldwide_Gross", "US_Gross + Worldwide_Gross as Total_Gross").show(false)

  //3
  moviesDF.select("Title", "Major_Genre", "IMDB_Rating").filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show(false)
  moviesDF.select("Title", "Major_Genre", "IMDB_Rating").where(column("Major_Genre") === "Comedy" and column("IMDB_Rating") > 6).show(false)


}
