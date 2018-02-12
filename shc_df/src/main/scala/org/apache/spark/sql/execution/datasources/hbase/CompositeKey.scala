package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.{DataFrame, SparkSession}

/*case class HBaseCompositeRecord(
                                 col00: String, // rowkey
                                 col1: String, // trajId
                                 col2: Float, // lat
                                 col3: Float, // lon
                                 col4: Float, // alt
                                 col5: Long, // ts
                                 col6: String // userId
                               )*/

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
                                  trajid: String
                                 //trajid: Long
                               )

/*

"rowkey":{"cf":"rowkey", "col":"ROWKEY", "type":"string", "length":"26" },
"plat":{"cf":"main", "col":"plat", "type":"float"},
"plon":{"cf":"main", "col":"plon", "type":"float"},
"lon":{"cf":"main", "col":"lon", "type":"float"},
"lat":{"cf":"main", "col":"lat", "type":"float"},
"dist":{"cf":"main", "col":"dist", "type":"double"},
"pts":{"cf":"main", "col":"pts", "type":"long"},
"ts":{"cf":"main", "col":"ts", "type":"long"},
"tdiffs":{"cf":"main", "col":"alt", "type":"long"},
"trajid":{"cf":"main", "col":"ts", "type":"string"}

*/

/*
create view "mvment" (ROWKEY VARCHAR PRIMARY KEY, "main"."trajId" VARCHAR,
"main"."lat" UNSIGNED_FLOAT, "main"."lon" UNSIGNED_FLOAT, "main"."alt" UNSIGNED_FLOAT,
"main"."ts" UNSIGNED_LONG , "main"."userId" VARCHAR);
*/

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
      // s"row${"%03d".format(i)}"
     //  s"String$i extra"
      i.toString
   //   i.toLong
    )
  }
}


object CompositeKey {


  /*
  create view "mvment" (ROWKEY VARCHAR PRIMARY KEY, "main"."trajId" VARCHAR,
  "main"."lat" UNSIGNED_FLOAT, "main"."lon" UNSIGNED_FLOAT, "main"."alt" UNSIGNED_FLOAT,
  "main"."ts" UNSIGNED_LONG , "main"."userId" VARCHAR);
  */

  /*  println("\n\n\n  ==start try_1_57 - try to read mvment keys with limited length    " +
      " result : no critical exceptions => output with UTF-8 problem => SUCCESS !!!   ***  \n\n\n")
    def cat = s"""{
                 |"table":{"namespace":"default", "name":"mvment", "tableCoder":"PrimitiveType"},
                 |"rowkey":"ROWKEY",
                 |"columns":{
                 |"rowkey":{"cf":"rowkey", "col":"ROWKEY", "type":"string", "length":"16" },
                 |"trajId":{"cf":"main", "col":"trajId", "type":"string"},
                 |"lat":{"cf":"main", "col":"lat", "type":"float"},
                 |"lon":{"cf":"main", "col":"lon", "type":"float"},
                 |"alt":{"cf":"main", "col":"alt", "type":"float"},
                 |"ts":{"cf":"main", "col":"ts", "type":"long"},
                 |"userId":{"cf":"main", "col":"userId", "type":"string"}
                 |}
                 |}""".stripMargin*/

  println("\n\n\n  ==start try_1_58 - try to read mvment keys with limited length    " +
    " result :  => SUCCESS !!!   ***  \n\n\n")

  /*  def cat =
      s"""{
         |"table":{"namespace":"default", "name":"mvment", "tableCoder":"PrimitiveType"},
         |"rowkey":"ROWKEY",
         |"columns":{
         |"rowkey":{"cf":"rowkey", "col":"ROWKEY", "type":"string", "length":"26" },
         |"trajId":{"cf":"main", "col":"trajId", "type":"string"},
         |"lat":{"cf":"main", "col":"lat", "type":"float"},
         |"lon":{"cf":"main", "col":"lon", "type":"float"},
         |"alt":{"cf":"main", "col":"alt", "type":"float"},
         |"ts":{"cf":"main", "col":"ts", "type":"long"},
         |"userId":{"cf":"main", "col":"userId", "type":"string"}
         |}
         |}""".stripMargin*/

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
       |"trajid":{"cf":"main", "col":"trajid", "type":"string" }
       |}
       |}""".stripMargin


  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("CompositeKeyExample")
      .getOrCreate()


    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    //populate table with composite key
    /* val data = (0 to 255).map { i =>
         HBaseCompositeRecord(i)
     }




      sc.parallelize(data).toDF.write.options(
        Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()*/

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    /*  //populate table with composite key
      val data = (0 to 255).map { i =>
          HBaseCompositeRecord(i)
      }
      sc.parallelize(data).toDF.write.options(
        Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()*/

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
    for (element <-list )
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

