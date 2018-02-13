package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.DataFrame

object JDBCTransport {

  def transport(rdbmsDataBaseName: String, tableName: String, dataFrame: DataFrame, appendMode: Boolean): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    val url = "jdbc:mysql://localhost:3306/" + rdbmsDataBaseName
    if (appendMode)
      dataFrame.write.mode("append").jdbc(url, tableName, prop)
    dataFrame.write.jdbc(url, tableName, prop)
  }

}
