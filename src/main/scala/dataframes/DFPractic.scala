package dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr}

object DFPractice extends App {

  val spark = SparkSession.builder().appName("Spark DataFrames Practice").master("local").getOrCreate()

  private val carsDF = spark.read.option("interSchema", "true").json("src/main/resources/data/cars.json")
  println(s"Counts of cars: ${carsDF.count()}")

  private val weight_in_kgs_expr: Column = carsDF.col("Weight_in_lbs") / 2.3

  private val carsWithWrightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weight_in_kgs_expr.as("Weight_in_kgs"),
    expr("Weight_in_lbs /2.2").as("Weight_in_kgs_2")
  )

  carsWithWrightDF.show(false)

  //DF processing
  //add new column to existing dataframe
  private val carsNewDF: DataFrame = carsDF.withColumn("Weight_in_kgs_3", col("Weight_in_lbs") / 2.2)

  private val carsNewDF2 = carsNewDF.withColumnRenamed("Weight_in_lbs", "Weight in Pounds")

  //accessing columns with white spaces in ColName
  carsNewDF2.selectExpr("`Weight in Pounds`").show(false)

  //remove a column
  println("Columns after dropping")
  carsNewDF2.drop("Origin", "Weight_in_kgs_3").show(false)

  //filtering
  carsDF.filter(col("Origin") =!= "USA").show(false)
  carsDF.where(col("Origin") === "USA").show(false)

  //filtering with expressions Strings
  carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150).show()
  carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150).show(false)
  carsDF.filter("Origin = 'USA' and Horsepower > 200").show(false)

  //union two dataframes (both dfs should have same schema)

  private val moreCarsDF = spark.read.option("interSchema", "true").json("src/main/resources/data/more_cars.json")
  private val allCars = carsDF.union(moreCarsDF)
  println(allCars.count())
  // distinct
  allCars.filter("Horsepower >200").select("Origin").distinct().show(false)

}
