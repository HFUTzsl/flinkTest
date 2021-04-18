package com.qf.bigdata.day2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable._
import org.apache.flink.api.scala._

object Demo1_DataSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //list源
    val list: ListBuffer[Int] = ListBuffer(10,20,30)
    val listDStream: DataStream[Int] = env.fromCollection(list).map(_ * 10)
    listDStream.print().setParallelism(1)
    println("=" * 100)

    //string源
    val stringDStream: DataStream[String] = env.fromElements("I love you")
    stringDStream.print().setParallelism(1)
    println("=" * 100)

    //file source
    val textDStream: DataStream[String] = env.readTextFile("C:\\real_win10\\day51-flink\\resource\\data.txt")
    textDStream.print().setParallelism(1)
    println("=" * 100)

    //socket source
    //      val socketDataStream: DataStream[String] = env.socketTextStream("ip", 1111)

    env.execute("job name")
  }
}