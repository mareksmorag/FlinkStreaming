package com.bigdata

import com.bigdata.datatypes.CrimesAgg
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class CrimesKeyedProcessFunction extends KeyedProcessFunction[String, CrimesAgg, CrimesAgg]{

  private var crimes_counter: ValueState[Int] = _
  private var arrest_counter: ValueState[Int] = _
  private var violation_counter: ValueState[Int] = _
  private var fbi_counter: ValueState[Int] = _

  override def processElement(i: CrimesAgg, context: KeyedProcessFunction[String, CrimesAgg, CrimesAgg]#Context, collector: Collector[CrimesAgg]): Unit = {
    val new_crimes_counter = crimes_counter.value + i.CrimesNo
    val new_arrest_counter = arrest_counter.value + i.ArrestNo
    val new_violation_counter = violation_counter.value + i.ViolationNo
    val new_fbi_counter = fbi_counter.value + i.FBINo

    crimes_counter.update(new_crimes_counter)
    arrest_counter.update(new_arrest_counter)
    violation_counter.update(new_violation_counter)
    fbi_counter.update(new_fbi_counter)

    collector.collect(CrimesAgg(
      i.Type,
      i.District,
      "",
      i.Month,
      new_crimes_counter,
      new_arrest_counter,
      new_violation_counter,
      new_fbi_counter
    ))
  }

  override def open(parameters: Configuration): Unit = {
    crimes_counter = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("crimes_counter", createTypeInformation[(Int)])
    )
    arrest_counter = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("arrest_counter", createTypeInformation[(Int)])
    )
    violation_counter = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("violation_counter", createTypeInformation[(Int)])
    )
    fbi_counter = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("fbi_counter", createTypeInformation[(Int)])
    )
  }
}
