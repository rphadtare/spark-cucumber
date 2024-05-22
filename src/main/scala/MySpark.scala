package main.scala

import org.apache.spark.sql.functions.{avg, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MySpark {

  val spark = SparkSession.builder().master("local[*]")
    .appName("MySpark").getOrCreate()

  def calculateAvgSalPerDept(empDf : DataFrame) : DataFrame = {

    empDf.select("dept_id", "salary").groupBy("dept_id")
      .agg(round(avg("salary"),2).as("avg_sal_per_dept"))
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val empDf = List( (101,"Rohit P", 10, 1000),
      (102,"Pooja P", 10, 1000),
      (103,"Rushi M", 10, 400),
      (104, "Rutu M", 20, 4000),
      (105, "Prithvi D", 20, 6000))
      .toDF("employee_id","employee_name","dept_id","salary")

    val df = calculateAvgSalPerDept(empDf)

    df.show(false)

    spark.close()
  }

}
