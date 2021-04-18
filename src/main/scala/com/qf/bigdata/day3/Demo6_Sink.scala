package com.qf.bigdata.day3

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Demo6_Sink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dStream: DataStream[String] = env.fromElements("李熙","利息","利息2","利息3")
    dStream.print() // 输出控制台
    //    dStream.writeAsText("file:///C:\\123.456")
    //    dStream.writeAsCsv("file:///C:\\456.799")
    dStream.writeToSocket("192.168.10.132", 6666, new SimpleStringSchema())

    env.execute("Demo6_Sink")
  }
}
