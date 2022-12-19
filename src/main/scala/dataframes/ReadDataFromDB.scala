package dataframes

import org.apache.spark.sql.SparkSession

object ReadDataFromDB extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .master("local")
    .getOrCreate()

  //private val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val dbConfig = Map(
    ("driver", driver),
    ("url", url),
    ("user", user),
    ("password", password),
    ("dbtable", "public.movies")
  )


  spark.read.format("jdbc").options(dbConfig).load().show()

}
