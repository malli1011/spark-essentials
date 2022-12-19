package dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}

object JoinsExercise extends App {

  /*
  - show all employees and their max salary
  - show all employees who were never managers
  - find the job titles of the best paid 10 employees in the company
   */

  val spark = SparkSession.builder().appName("Joins Exercise").master("local").getOrCreate()

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

  def readTabele(tableName: String): DataFrame = {
    spark.read.format("jdbc").options(dbConfig).option("dbtable", s"public.$tableName").load()
  }


  val employeesDF = readTabele("employees")
  val managersDF = readTabele("departments")
  val salariesDF = readTabele("salaries")
  val titlesDF = readTabele("titles")
  val deptMgrDF = readTabele("dept_manager")

  //  employeesDF.show(false)
  //  managersDF.show(false)
  //  salariesDF.show(false)

  //1
  val employeesWithMaxSalDF = salariesDF.groupBy("emp_no").max("salary")
  employeesDF.join(employeesWithMaxSalDF, "emp_no")
  //.show()

  //2
  val condition = employeesDF.col("emp_no") === deptMgrDF.col("emp_no")
  employeesDF.join(deptMgrDF, condition, "left_anti").drop(deptMgrDF.col("emp_no"))
  //.show()

  //3

  val employeesWithMaxSalDF2 = employeesDF.join(salariesDF.groupBy("emp_no").agg(
    max("salary").as("maxSalary")
  ), "emp_no")
  //val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no").max("to_date")
  //the above line max function will work only with numeric columns but not for date. hence use max with agg functions to get max of date columns.
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesWithMaxSalDF2.orderBy(col("maxSalary").desc_nulls_last).limit(10)

  bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no").show()

}
