/*
 * (C) 2017 Hortonworks, Inc. All rights reserved. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. This file is licensed to You under the Apache License, Version 2.0
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
 */

package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HBaseCompositeRecord(
    col00: String,      // rowkey
    col1: String,       // trajId
    col2: Float,        // lat
    col3: Float,        // lon
    col4: Float,        // alt
    col5: Long,         // ts
    col6: String        // userId
                               )

/*
create view "mvment" (ROWKEY VARCHAR PRIMARY KEY, "main"."trajId" VARCHAR,
"main"."lat" UNSIGNED_FLOAT, "main"."lon" UNSIGNED_FLOAT, "main"."alt" UNSIGNED_FLOAT,
"main"."ts" UNSIGNED_LONG , "main"."userId" VARCHAR);
*/

object HBaseCompositeRecord {
  def apply(i: Int): HBaseCompositeRecord = {
    HBaseCompositeRecord(
      s"row${"%03d".format(i)}",
      s"String$i extra",
      i.toFloat,
      i.toFloat,
      i.toFloat,
      i.toLong,
      s"String$i extra"
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
  def cat = s"""{
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
               |}""".stripMargin






  def main(args: Array[String]){
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
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
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
    if(df.count() != 256){
      throw new Exception("value invalid")
    }


    //filtered query7. The number of results is 17
    df
       // .select("col00", "col01","col1")
      .select("col1","col2")
        .show

    spark.stop()
  }
}

