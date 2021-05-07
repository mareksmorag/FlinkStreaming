package com.bigdata

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import datatypes.{AnomalyAgg, Crimes, CrimesAgg}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.ArrayList
import scala.collection.mutable.Map

object CrimesDescriber {
  def getIUCRData(filepath: String): Map[String, (_, _)] = {
    var IUCRData =Map[String, (_,_)]()
    val file_reader= io.Source.fromFile(filepath)
    for(line <- file_reader.getLines){
      val line_elements = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      var id = line_elements(0)
      if (line_elements(0).length==3) {
        id = "0" + id
      }
      if(line_elements.length == 4) {
        IUCRData += (id -> (line_elements(1),line_elements(3)))
      }
    }
    IUCRData.-("IUCR")
    file_reader.close
    IUCRData
  }


  def main(args: Array[String]) : Unit = {
    val D = args(0)
    val L = args(1)
    val IUCR_file = args(2)
    val topic_name =args(3)


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "testGroup")

//    val text: DataStream[String] = env
//      .addSource(new FlinkKafkaConsumer[String]("kafka-tt", new SimpleStringSchema(), properties))

    val df = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")
    val df2 = new SimpleDateFormat("MM/yyyy")
    val IUCR: Map[String, (_,_)] = getIUCRData(IUCR_file)
//    val text: DataStream[Crimes] = env.readTextFile(filepath)
    val text: DataStream[Crimes] = env
      .addSource(new FlinkKafkaConsumer[String](topic_name, new SimpleStringSchema(), properties))
      .filter(!_.startsWith(",ID"))
      .map(a => a.split(",(?=([^\"]|\"[^\"]*\")*$)"))
      .filter(_.length==23)
      .filter(array => IUCR.contains(array(5)))
      .map(array => Crimes(array(1).toLong, array(3),
        df2.format(df.parse(array(3))),
        IUCR(array(5))._1.toString,
        IUCR(array(5))._2.toString, array(9).toBoolean,
        array(10).toBoolean,
        array(12),
        array(14),
        array(20).toDouble,
        array(21).toDouble))


//    text.print().setParallelism(1)

    val wTaWCrimesDS: DataStream[Crimes] = text.assignTimestampsAndWatermarks(new MyWatermarkGenerator)

//    wTaWCrimesDS.print().setParallelism(1)

//    val finalDS: DataStream[CrimesAgg] = wTaWCrimesDS
//      .keyBy(cr => cr.Type + ":" + cr.District)
//      .window(TumblingEventTimeWindows.of(Time.days(1)))
//      .aggregate(new MyAggFun)

    val finalDS: DataStream[CrimesAgg] = wTaWCrimesDS
      .keyBy(cr => cr.Month + ":" + cr.Type + ":" + cr.District)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .aggregate(new MyAggFun)
      .keyBy(cr => cr.Month + cr.Type + cr.District)
      .process(new CrimesKeyedProcessFunction)


    finalDS.print().setParallelism(1)

    val anomalyDS: DataStream[AnomalyAgg] = wTaWCrimesDS
      .keyBy(cr => cr.District)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .aggregate(new MyAggFun)
      .keyBy(cr => cr.District)
      .window(SlidingEventTimeWindows.of(Time.days(D.toInt), Time.days(1)))
      .process(new ProcessAnomalies)
      .filter(_.Percentage >= L.toDouble)

    anomalyDS.print().setParallelism(1)


    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[CrimesAgg](
      httpHosts,
      new ElasticsearchSinkFunction[CrimesAgg] {
        def process(element: CrimesAgg, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String, Any]
          json.put("Type", element.Type)
          json.put("Distinct", element.District)
          json.put("Month", element.Month)
          json.put("CrimesNo", element.CrimesNo)
          json.put("ArrestNo", element.ArrestNo)
          json.put("ViolationNo", element.ViolationNo)
          json.put("FBINo", element.FBINo)

          val rqst: IndexRequest = Requests.indexRequest
            .index("crimes-sink")
            .source(json)

          indexer.add(rqst)
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(1)
    val esSinkAnomalyBuilder = new ElasticsearchSink.Builder[AnomalyAgg](
      httpHosts,
      new ElasticsearchSinkFunction[AnomalyAgg] {
        def process(element: AnomalyAgg, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String, Any]
          json.put("Start", element.Start)
          json.put("Stop", element.Stop)
          json.put("District", element.District)
          json.put("CrimesNo", element.CrimesNo)
          json.put("FBINo", element.FBINo)
          json.put("Percentage", element.Percentage)

          val rqst: IndexRequest = Requests.indexRequest
            .index("anomalies-sink")
            .source(json)

          indexer.add(rqst)
        }
      }
    )
    esSinkAnomalyBuilder.setBulkFlushMaxActions(1)

    finalDS.addSink(esSinkBuilder.build)
    anomalyDS.addSink(esSinkAnomalyBuilder.build)
    env.execute("XD")
  }


}
