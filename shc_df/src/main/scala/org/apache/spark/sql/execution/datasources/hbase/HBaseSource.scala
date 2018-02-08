/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase.examples

import org.apache.spark.sql.execution.datasources.hbase.{HBaseRecord, _}
import org.apache.spark.sql.{DataFrame, SparkSession}


case class HBaseRecord(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte)

object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

/*case class HBaseRecord(
                        col0: String,
                        col1: String,
                        col2: String,
                        col3: String,
                        col4: String,
                        col5: String,
                        col6: String,
                        col7: String,
                        col8: String

                      )

object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(
      s,
      s"String$i extra",
      s"String$i extra",
      s"String$i extra",
      s"String$i extra",
      s"String$i extra",
      s"String$i extra",
      s"String$i extra",
      s"String$i extra"

    )
  }
}*/

object HBaseSource {


   println("\n\n\n\n\n\n ************************* ==start try_1_34 invalid  ************************* \n\n\n\n\n")
  def cat = s"""{
               |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
               |"rowkey":"key1:key2",
               |"columns":{
               |"col00":{"cf":"rowkey", "col":"key1", "type":"string", "length":"6"},
               |"col01":{"cf":"rowkey", "col":"key2", "type":"int"},
               |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
               |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
               |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
               |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
               |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
               |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
               |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
               |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
               |}
               |}""".stripMargin










  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("HBaseSourceExample")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setLogLevel("ERROR")

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val data = (0 to 255).map { i =>
      HBaseRecord(i)
    }

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


    val df = withCatalog(cat)
    df.show
    df.createOrReplaceTempView("table1")
    val c1 = sqlContext.sql("select count(*) as count_all from table1 ")
    c1.show()
    spark.stop()
  }
}



/*
  def cat = s"""{
               |"table":{"namespace":"default", "name":"WEB_STAT", "tableCoder":"PrimitiveType"},
               |"rowkey":"HOST:DOMAIN:FEATURE:DATE",
               |"columns":{
               |"col00":{"cf":"rowkey", "col":"HOST", "type":"string", "length":"16"},
               |"col01":{"cf":"rowkey", "col":"DOMAIN", "type":"string", "length":"16"},
               |"col02":{"cf":"rowkey", "col":"FEATURE", "type":"string", "length":"16"},
               |"col03":{"cf":"rowkey", "col":"DATE", "type":"bigint"},
               |"col1":{"cf":"USAGE", "col":"DB", "type":"bigint"},
               |"col2":{"cf":"USAGE", "col":"CORE", "type":"bigint"},
               |"col3":{"cf":"STATS", "col":"ACTIVE_VISITOR", "type":"int"}
               |}
               |}""".stripMargin*/

/*def cat = s"""{
             |"table":{"namespace":"default", "name":"WEB_STAT", "tableCoder":"PrimitiveType"},
             |"columns":{
             |"col1":{"cf":"main", "col":"userId", "type":"string" , "length":"16"},
             |"col2":{"cf":"main", "col":"alt", "type":"string" , "length":"16"},
             |"col3":{"cf":"main", "col":"log", "type":"string" , "length":"16"},
             |"col4":{"cf":"main", "col":"trajId", "type":"string" , "length":"16"},
             |"col5":{"cf":"main", "col":"ts", "type":"string" , "length":"16"},
             |"col6":{"cf":"main", "col":"lat", "type":"string" , "length":"16"}
             |}
             |}""".stripMargin*/

/*  println("\n\n\n\n\n\n ************************* ==start try_1_23 ************************* \n\n\n\n\n")
  def cat = s"""{
                  |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
                  |"columns":{
                  |"col1":{"cf":"main", "col":"userId", "type":"string" },
                  |"col2":{"cf":"main", "col":"alt", "type":"string" },
                  |"col3":{"cf":"main", "col":"log", "type":"string" },
                  |"col4":{"cf":"main", "col":"trajId", "type":"string"},
                  |"col5":{"cf":"main", "col":"ts", "type":"string" },
                  |"col6":{"cf":"main", "col":"lat", "type":"string"}
                  |}
               |}""".stripMargin*/

/*  println("\n\n\n\n\n\n ************************* ==start try_1_28 ************************* \n\n\n\n\n")
  def cat = s"""{
               |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
               |"columns":{
               |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
               |"col1":{"cf":"cf1", "col":"col1", "type":"string" },
               |"col2":{"cf":"cf2", "col":"col2", "type":"string" },
               |"col3":{"cf":"cf3", "col":"col3", "type":"string" },
               |"col4":{"cf":"cf4", "col":"col4", "type":"string" },
               |"col5":{"cf":"cf5", "col":"col5", "type":"string" },
               |"col6":{"cf":"cf6", "col":"col6", "type":"string" },
               |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
               |"col8":{"cf":"cf8", "col":"col8", "type":"string"}
               |}
               |}""".stripMargin*/

/* println("\n\n\n\n\n\n ************************* ==start try_1_30 ************************* \n\n\n\n\n")
  val cat =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
       |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
       |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
       |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
       |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
       |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
       |"col8":{"cf":"cf8", "col":"col8", "type":"string"}
       |}
       |}""".stripMargin*/

/* println("\n\n\n\n\n\n ************************* ==start try_1_31 valid  ************************* \n\n\n\n\n")
 val cat =
   s"""{
      |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
      |"rowkey":"key",
      |"columns":{
      |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
      |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
      |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
      |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
      |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
      |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
      |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
      |"col7":{"cf":"cf7", "col":"col7", "type":"string"}
      |
      |}
      |}""".stripMargin*/

/* println("\n\n\n\n\n\n ************************* ==start try_1_32 invalid **" + "Exception in thread \"main\" " +
   "java.lang.UnsupportedOperationException: empty.tail" +
   "*********************** \n\n\n\n\n")
val cat =
  s"""{
     |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
     |"rowkey":"key",
     |"columns":{
     |
     |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
     |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
     |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
     |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
     |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
     |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
     |"col7":{"cf":"cf7", "col":"col7", "type":"string"}
     |
     |}
     |}""".stripMargin*/

/*println("\n\n\n\n\n\n ************************* ==start try_1_33" +
  "invalid Exception in thread \"main\" java.util.NoSuchElementException: None.get " +
  " ************************* \n\n\n\n\n")
val cat =
s"""{
  |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
  |
  |"columns":{
  |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
  |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
  |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
  |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
  |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
  |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
  |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
  |"col7":{"cf":"cf7", "col":"col7", "type":"string"}
  |
  |}
  |}""".stripMargin*/