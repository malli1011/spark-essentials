package dataframes

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object Joins extends App {

  val spark = SparkSession.builder().appName("Joins").master("local").getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")

  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")
  val playersDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")

  //joins

  //Inner joins
  private val condition = playersDF.col("band") === bandsDF.col("id")
  //  playersDF.join(bandsDF,condition,"inner")

  //default join is Inner
  playersDF.join(bandsDF, playersDF.col("band") === bandsDF.col("id"), "inner")
  //.show(false)

  //Outer Joins
  //left outer = everything in the inner join + all the rows in the left table, with nulls in where the data is missing
  playersDF.join(bandsDF, condition, "left_outer")
  //.show(false)

  //right outer
  playersDF.join(bandsDF, condition, "right_outer")
  //.show(false)

  //full outer join
  playersDF.join(bandsDF, condition, "outer")
  //.show(false)

  //semi-joins  // shows only the columns from left table which are matching the given condition
  playersDF.join(bandsDF, condition, "left_semi")
  //.show(false)

  //anti-join //shows only columns from left table which doesn't satisfy the condition
  playersDF.join(bandsDF, condition, "left_anti")
  //.show(false)

  //things to bear in mind
  private val joinDF: DataFrame = playersDF.join(bandsDF, playersDF.col("band") === bandsDF.col("id"))

  //the below line will crash because after the join the resulting DF will have two columns with name 'id'. Both palyersDF and bandsDF has column with name 'id'
  //joinDF.select("id","band").show()
  //option 1 - rename the column on which we are joining
  playersDF.withColumnRenamed("id", "player_id")
    .join(bandsDF.withColumnRenamed("id", "band").withColumnRenamed("name", "band_name"), "band")
    .show()

  //option2 //drop the dupe column
  joinDF.drop(bandsDF.col("id"))

  //using complex types eg: join using an array
  playersDF.join(guitarsDF.withColumnRenamed("id","guitarId"), expr("array_contains(guitars, guitarId"))


}
