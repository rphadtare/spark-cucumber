package steps

import io.cucumber.datatable.DataTable
import io.cucumber.scala.{EN, PendingException, ScalaDsl}
import main.scala.MySpark.calculateAvgSalPerDept
import org.apache.spark.sql.DataFrame
import steps.SparkTestHelper.{close_spark, dataTableToDataframe}

class StepDefinitions extends ScalaDsl with EN {

  var empDf: DataFrame = null
  var avg_sal_per_dept_df: DataFrame = null

  Given("""there is {string} dataframe with following data:""") { (string: String, dataTable: io.cucumber.datatable.DataTable) =>
    empDf = dataTableToDataframe(dataTable)
    println("Scenario: Calculating average salary per dept when employee data is present")
    println("Employee dataframe with data - ")
    empDf.show(false)
  }
  When("""calculating average salary per department""") { () =>
    // Write code here that turns the phrase above into concrete actions
    avg_sal_per_dept_df = calculateAvgSalPerDept(empDf)
    println("Calculated average salary per department dataframe - ")
    avg_sal_per_dept_df.show(false)
  }
  Then("""result is {string} dataframe with following lines:""") { (string: String, dataTable: io.cucumber.datatable.DataTable) =>
    // Write code here that turns the phrase above into concrete actions
    val expected_df = dataTableToDataframe(dataTable)
    println("Expected average salary per department dataframe - ")
    expected_df.show(false)
    val cnt = expected_df.exceptAll(avg_sal_per_dept_df).count()
    assert(cnt == 0, message = "Records are not matching from expected average salary per dept dataframe with calculated dataframe !!")
    close_spark
  }
}