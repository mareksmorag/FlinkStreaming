package com.bigdata

import datatypes.{Crimes, CrimesAgg}
import org.apache.flink.api.common.functions.AggregateFunction

import java.text.SimpleDateFormat

class MyAggFun extends AggregateFunction[Crimes, CrimesAgg, CrimesAgg]{
  override def createAccumulator(): CrimesAgg = CrimesAgg("","","","",0,0,0,0)

  override def add(in: Crimes, acc: CrimesAgg): CrimesAgg = CrimesAgg(
    if (acc.Type.isEmpty) in.Type else acc.Type,
    if (acc.District.isEmpty) in.District else acc.District,
    if (acc.Day.isEmpty) new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a").parse(in.Date).formatted("%tF") else acc.Day,
    if (acc.Month.isEmpty) in.Month else acc.Month,
    acc.CrimesNo + 1,
    if(in.Arrest) acc.ArrestNo + 1 else acc.ArrestNo,
    if(in.Domestic) acc.ViolationNo + 1 else acc.ViolationNo,
    if(in.IUCR == "I") acc.FBINo + 1 else acc.FBINo
  )

  override def getResult(acc: CrimesAgg): CrimesAgg = acc

  override def merge(acc: CrimesAgg, acc1: CrimesAgg): CrimesAgg = CrimesAgg(
    acc.Type,
    acc.District,
    acc.Day,
    acc.Month,
    acc.CrimesNo + acc1.CrimesNo,
    acc.ArrestNo + acc1.ArrestNo,
    acc.ViolationNo + acc1.ViolationNo,
    acc.FBINo + acc1.FBINo
  )
}
