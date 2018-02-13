package org.apache.spark.sql.execution.datasources.hbase

case class EnrichedRecord(
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


object EnrichedRecord {
  def apply(i: Int): EnrichedRecord = {
    EnrichedRecord(
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