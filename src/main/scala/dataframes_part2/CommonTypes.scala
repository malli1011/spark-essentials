package dataframes_part2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App")
    .master("local[1]")
    .getOrCreate()


  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //adding a plain value as column to the DF
  moviesDF.select(col("Title"), lit(100).as("plain_text"))
  //.show

  //Boolean
  val dramFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val prefferedFilter = dramFilter.and(goodRatingFilter)
  moviesDF.where(prefferedFilter)
  //.show()
  val goodMoviesDF = moviesDF.select(col("Title"), prefferedFilter.as("Good_movie"))
  //.show()

  //we can use a boolean column inside where
  goodMoviesDF.where("Good_movie")
  goodMoviesDF.where(not(col("Good_movie")))

  //math operations
  val moviesAvgRatingsDF = moviesDF
    .select(col("Title"),
      ((col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2).as("Avg_Rating"))
  //.show()


  //String operations: Capitalization (initcap,lower,upper)
  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  carsDF.withColumn("Name", initcap(col("Name"))).drop(carsDF.col("Name"))
  //.show()
  carsDF.withColumn("Name", upper(col("Name"))).drop(carsDF.col("Name"))
  //.show()

  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))
  //.show()

  //regex
  val regexString = "volkswagen|vw"
  carsDF.select(
    col("*"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
  //.show()


  carsDF.select(
    col("*"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_extract")
  ).where(col("regex_extract") =!= "")
  //.show(100)

  /**
    * Filter the cars DF by a list of car names obtained by an API call
    * versions :
    * -contains
    * -regex
    */

  def getCarNames: List[String] = List("Volkswagen", "Ford")

  val regExp = getCarNames.map(_.toLowerCase()).mkString("|")
  carsDF.select(
    col("*"),
    regexp_extract(col("Name"), regExp, 0).as("reg_exp")
  ).where(col("reg_exp") =!= "").show(false)
}
