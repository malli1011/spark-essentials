package datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}

import java.util.Date

object DataSets2 extends App {

  val spark = SparkSession.builder()
    .appName("Spark Essentials DataSets")
    .master("local[1]")
    .getOrCreate()

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  import spark.implicits._

  val carsDS = carsDF.as[Car]

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */


  //1
  val carsCount = carsDS.count
  println(carsDS.count())
  //2
  carsDS.filter(_.Horsepower.getOrElse(0L) > 140).show()

  //3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower"))).show()

  // Grouping DS
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()
}
