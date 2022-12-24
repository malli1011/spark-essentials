package spark_sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import spark_sql.SparkSql.{readTable, spark}

object SqlExample extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse") //override the default warehouse path to use a user specified dir.
    .master("local")
    .getOrCreate()

  spark.sql("create database rtjvm")
  spark.sql("""use rtjvm""")

  val list = List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")

  //list.foreach(table => {spark.sql(s"""select * from ${table}""".stripMargin).show(false)})

  // read DF from loaded Spark tables
  val employeesDF2 = spark.read.option("path", "src/main/resources/warehouse").table("rtjvm.employees")
  employeesDF2.show(false)


  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // 2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      """.stripMargin
  )

}
