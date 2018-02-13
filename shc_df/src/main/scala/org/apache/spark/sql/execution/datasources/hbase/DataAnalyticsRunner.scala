package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseSparkConfig.spark


object DataAnalyticsRunner {

  def runAnalytics(): Unit = {
    import HBaseSparkConfig._
    //full query
    val df = withCatalog(cat)
    df.show
    val table2 = describeDataAnalytics(df, "mvment_by_traj")
    printResult(table2)
    spark.stop()
  }

  def describeDataAnalytics(df: DataFrame, viewName: String): DataFrame = {
    df.createOrReplaceTempView(viewName)

    val test_query = "select distinct(rowkey) as distinctRowKey from " + viewName + " limit 24 "

    val table2 = spark.sql(test_query)
    table2.show()
    println(" === users_by_collection_period === ")
    val users_by_collection_period =
      "select userId, min(ts), max(ts) from mvment_by_traj group by  userId "
    val table3 = spark.sql(users_by_collection_period)
    table3.show()
    println(" === user_by_trajectories === ")
    val user_by_trajectories =
      "select userId, count(distinct trajId) as amount from mvment_by_traj group by userId order by amount desc limit 10  "
    val table4 = spark.sql(user_by_trajectories)
    table4.show()
    println(" === trajectories_by_duration === ")
    val trajectories_by_duration =
      "select trajid, sum(tdiffs) duration from mvment_by_traj group by trajid order by duration desc limit 10 "
    val table5 = spark.sql(trajectories_by_duration)
    table5.show()
    println(" === trajectories_by_distance === ")
    val trajectories_by_distance =
      "select trajid, sum(dist) distance from mvment_by_traj group by trajid order by distance desc limit 10"
    val table6 = spark.sql(trajectories_by_distance)
    table6.show()



    table2
  }

  def printResult(df: DataFrame): Unit = {
    val list = df.collect()
    println(list)
    for (element <- list)
      println(element)

  }


  def main(args: Array[String]) {

    runAnalytics()
  }
}

