package com.bigdata

import datatypes.Crimes
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

import java.text.SimpleDateFormat
import scala.math.max

class MyWatermarkGenerator extends AssignerWithPeriodicWatermarks[Crimes]{
  def getTimestamp(s: String): Long = {
    val format = new SimpleDateFormat("MM/dd/yyy hh:mm:ss a")
    format.parse(s).getTime
  }
  val maxOutOfOrderness = 6000L
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(te: Crimes, previousElementTimestamp: Long): Long = {
    val timestamp = getTimestamp(te.Date)
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}
