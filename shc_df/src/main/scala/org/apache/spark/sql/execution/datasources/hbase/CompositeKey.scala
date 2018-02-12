package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.{DataFrame, SparkSession}


case class HBaseCompositeRecord(
                                 rowkey: String,
                                 plat: Float,
                                 plon: Float,
                                 lon: Float,
                                 lat: Float,
                                 dist: Double,
                                 pts: Long,
                                 ts: Long,
                                 tdiffs: Long,
                                 trajid: String,
                                 userId: String
                               )


object HBaseCompositeRecord {
  // s"row${"%03d".format(i)}",
  def apply(i: Int): HBaseCompositeRecord = {
    HBaseCompositeRecord(
      s"String$i extra",
      i.toFloat,
      i.toFloat,
      i.toFloat,
      i.toFloat,
      i.toDouble,
      i.toLong,
      i.toLong,
      i.toLong,
      i.toString,
      i.toString
    )
  }
}


object CompositeKey {


  println("\n\n\n  ==start try_1_58 - try to read mvment keys with limited length    " +
    " result :  => SUCCESS !!!   ***  \n\n\n")


  def cat: String =
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


  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("CompositeKeyExample")
      .getOrCreate()


    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext


    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }


    //full query
    val df = withCatalog(cat)
    df.show


    df.createOrReplaceTempView("mvment_t")

    //filtered query7. The number of results is 17
    //val table = spark.sql("select * from mvment_t limit 24")
    val table1 = spark.sql("select tdiffs from mvment_t limit 24")
    table1.show(22)

    val table2 = spark.sql("select distinct(rowkey) as distinctRowKey from mvment_t limit 24")
    table2.show(22)
    val list = table2.collect()
    // list.forEach(x=> println(x))
    // println(list  "\n")
    println(list)
    for (element <- list)
      println(element)


    //compositeKey

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //jdbc mysql url - destination database is named "data"
    val url = "jdbc:mysql://localhost:3306/data"

    //destination database table


    //write data from spark dataframe to database
    //to add new data
    /*
    *
    * table.write.mode("append").jdbc(url, "mytable", prop)
    *
    * */


    //re - write the data
    /*
    *
    * table.write.jdbc(url, "mvment_test1", prop)
    *
    */



    spark.stop()
  }
}

