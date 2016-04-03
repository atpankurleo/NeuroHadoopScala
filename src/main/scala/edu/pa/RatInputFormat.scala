package edu.pa
/**
  * Created by Ankur on 3/25/2016.
  */
class RatInputFormat extends Serializable{
  var timestamp: String = null
  var voltage: String = String.valueOf(0)

  def getVoltage: Short = {
    return this.voltage.toShort
  }

  def getTimestamp: Long = {
    return this.timestamp.toLong
  }

  def parse(csvRow: String): RatInputFormat = {
    val rec: RatInputFormat = new RatInputFormat
    val values: Array[String] = csvRow.split(",")
    if (values.length != 2) {
      return null
    }
    rec.timestamp = values(0).trim
    rec.voltage = values(1).trim
    return rec
  }
}
