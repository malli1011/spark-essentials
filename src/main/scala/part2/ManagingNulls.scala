package part2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
  val spark = SparkSession.builder().appName("Managing nulls").master("local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //coalesce

  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Ratting"))
    .orderBy(col("Ratting").desc_nulls_last)
  //.show(50, false)

  //checking for nulls
  moviesDF.select("*").filter(col("Director").isNotNull)
  //.show(false)

  //removing nulls
  //na.drop removes rows containing nulls in the selected cols
  moviesDF.select("Title", "IMDB_Rating", "Director").na.drop()
  //.show(100, false)

  //replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")) // replaces null with 0 in cols "IMDB_Rating","Rotten_Tomatoes_Rating")

  //we can use a map to replace null values for each of the column
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  )).select("Title", "IMDB_Rating", "Rotten_Tomatoes_Rating","Director")
    .show(false)

  moviesDF.selectExpr(
    "Title",
    "Director",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as Ratting", //same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl_Ratting", //same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // retuns null if the two values are EQUAL else return first valu
    "nvl2(Rotten_Tomatoes_Rating, Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl2" // if (firstArg !=null) return secondArg else return thirdArg
  )
  //.show()
}
