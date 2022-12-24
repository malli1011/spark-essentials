package rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

import scala.io.Source

object rdd extends App {
  //Resilient Distributed Datasets
  //distributed typed collections of JVM objects
  //All higher-level APIs reduce to RDDs.

  /* Essential proficiency
  - Read from external sources
  - Convert to/from DataFrames and Datasets
  - diff b/w RDDs/DF/DS
  */

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  private val sparkContext: SparkContext = spark.sparkContext

  private val numbers: Range.Inclusive = 1 to 10000

  //parallelize and existing collection
  private val numbersRDD: RDD[Int] = sparkContext.parallelize(numbers)
  //numbersRDD.filter(_ > 500).foreach(println)

  case class StockValue(symbol: String, date: String, price: Double)

  def readCSVFile(fileName: String): List[StockValue] = {
    val source = Source.fromFile(fileName)
    try {
      source.getLines().drop(1).map(_.split(",")).map(t => StockValue(t(0), t(1), t(2).toDouble)).toList
    } finally {
      source.close()
    }

  }
 //create RDD from csv
  val data = readCSVFile("src/main/resources/data/stocks.csv")
  private val stocksRDD: RDD[StockValue] = sparkContext.parallelize(data)


  //Create RDD from DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you get to keep type info

  // Transformations

  // distinct
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  // counting
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive

  // Partitioning

  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /*
    Repartitioning is EXPENSIVE. Involves Shuffling.
    Best practice: partition EARLY, then process that.
    Size of a partition 10-100MB.
   */

  // coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
    * Exercises
    *
    * 1. Read the movies.json as an RDD.
    * 2. Show the distinct genres as an RDD.
    * 3. Select all the movies in the Drama genre with IMDB rating > 6.
    * 4. Show the average rating of movies by genre.
    */

  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}

/*
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
 */

