package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object HBaseSparkConfig {

  def describeHBaseTable(): String = {

    s"""{
       |"table":{"namespace":"default", "name":"mvment_by_traj", "tableCoder":"PrimitiveType"},
       |"rowkey":"ROWKEY",
       |"columns":{
       |"rowkey":{"cf":"rowkey", "col":"ROWKEY", "type":"string", "length":"128" },
       |"plat":{"cf":"main", "col":"plat", "type":"float"},
       |"plon":{"cf":"main", "col":"plon", "type":"float"},
       |"lon":{"cf":"main", "col":"lon", "type":"float"},
       |"lat":{"cf":"main", "col":"lat", "type":"float"},
       |"dist":{"cf":"main", "col":"dist", "type":"double"},
       |"pts":{"cf":"main", "col":"pts", "type":"long"},
       |"ts":{"cf":"main", "col":"ts", "type":"long"},
       |"tdiffs":{"cf":"main", "col":"tdiffs", "type":"long"},
       |"trajid":{"cf":"main", "col":"trajid", "type":"string" },
       |"userId":{"cf":"main", "col":"userId", "type":"string" }
       |}
       |}""".stripMargin
  }

  val spark: SparkSession = SparkSession.builder()
    .appName("CompositeKeyExample")
    .getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

  val cat: String = describeHBaseTable()

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

}
