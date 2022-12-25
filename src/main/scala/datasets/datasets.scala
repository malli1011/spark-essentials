package datasets

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object datasets extends App {
  val spark = SparkSession.builder()
    .appName("Spark Essentials DataSets")
    .master("local[1]")
    .getOrCreate()


  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  moviesDF.select("Title", "Director", "Distributor").na.drop().show(false)

  //Create a DataSet
  //1. Define a case class
  //dataset of a complex type
  case class Band(var id: Long,
                  var name: String,
                  var hometown: String,
                  var year: Option[Long] // use Option to handle nulls
                 )

  //2. Read the DF
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")
  //3.  Define an encode
  implicit val bandEncoder: Encoder[Band] = Encoders.product[Band]
  //4. Convert Df to DS
  val bandsDS = bandsDF.as[Band]
  bandsDS.filter(_.name.toLowerCase().startsWith("s")).show()

}
