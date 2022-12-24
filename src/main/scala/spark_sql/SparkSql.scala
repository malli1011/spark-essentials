package spark_sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark_sql.SqlExample.spark

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse") //override the default warehouse path to use a user specified dir.
    //.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .master("local")
    .getOrCreate()

  //the below option doesn't work for spark 3.0 instead specify the path while saving the DF to a table as below to allow the override to work
  //spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

  //employeesDF.write.mode(SaveMode.Overwrite).option("path","src/main/resources/warehouse").saveAsTable("employees")

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  //Query a Regular DF
  carsDF.select("Name").where("Origin = 'USA'")
  //.show(false)

  //Query using spark SQL
  carsDF.createOrReplaceTempView("cars")
  private val americanCarsDF = spark.sql(""" select Name,Origin from cars where Origin='USA'""".stripMargin)
  //americanCarsDF.show(false)

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  private val databasesDF: DataFrame = spark.sql("show databases")
  databasesDF.show(false)

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val dbConfig = Map(
    ("driver", driver),
    ("url", url),
    ("user", user),
    ("password", password)
  )

  def readTable(tableName: String): DataFrame = {
    spark.read.format("jdbc").options(dbConfig).option("dbtable", s"public.$tableName").load()
  }

  //transfer tables from a DB to Spark table

  /*val employeesDF = readTable("employees")
  employeesDF.createOrReplaceTempView("employees")

  employeesDF.write.mode(SaveMode.Overwrite).option("path", "src/main/resources/warehouse").saveAsTable("employees")

  private val table = "employees"
  spark.sql(s"""select * from ${table}""".stripMargin).show()
*/
  private def transferTables(tableNames: List[String]): Unit = tableNames.foreach {
    tableName =>
      val df = readTable(tableName)
      df.createOrReplaceTempView(tableName)
      df.write.option("path", "src/main/resources/warehouse").mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    //spark.sql(s"""select * from ${tableName}""".stripMargin).show()
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // 2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      """.stripMargin
  ).show(false)

  // 3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
      """.stripMargin
  )

  // 4
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      """.stripMargin
  ).show()

}
