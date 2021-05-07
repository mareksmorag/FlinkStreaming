package com.bigdata

import com.bigdata.datatypes.{AnomalyAgg, CrimesAgg}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date
class ProcessAnomalies extends ProcessWindowFunction[CrimesAgg,AnomalyAgg,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[CrimesAgg], out: Collector[AnomalyAgg]): Unit = {

    var crimesCounter = 0
    var FBICounter = 0
    for (e <- elements){
      crimesCounter += e.CrimesNo
      FBICounter += e.FBINo
    }

    val acc = AnomalyAgg(
      new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a").format(new Date(context.window.getStart)),
      new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a").format(new Date(context.window.getEnd)),
      key,
      crimesCounter,
      FBICounter,
      (FBICounter.toDouble/crimesCounter.toDouble) * 100
    )

    out.collect(acc)
  }
}

