package model


import java.text.SimpleDateFormat


case class Reading(userId: String, trajectoryId: String, lat: Float, lon: Float,
                   alt: Float, date: String, time: String) extends java.io.Serializable {

  val ts: Long = new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss").parse(date + "," + time).getTime

  def toTuple: (String, String, Float, Float, Float, Long, String) = {
    (userId + "|" + (Long.MaxValue - ts).toString, trajectoryId, lat, lon, alt, ts, userId)
  }
}