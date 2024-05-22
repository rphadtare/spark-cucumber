package steps

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import io.cucumber.datatable.DataTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
object SparkTestHelper {

  val spark = SparkSession.builder().master("local[*]").appName("SparkTestHelper")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def dataTableToDataframe(table: DataTable): DataFrame = {
    import spark.implicits._
    val columns: Seq[String] = table.cells().head.toSeq
    val data = table.cells().drop(1).toSeq.map(r => r.toList)
    data.toDF().select(columns.indices.map(i => col("value")(i).alias(columns(i))): _*)
  }

  def close_spark()={
    spark.close()
  }

}
