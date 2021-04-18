package com.qf.bigdata.day3

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.util.Random

object Demo1_CustomSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.addSource(new MySourceFunction)
    dataStream.print().setParallelism(1)
    env.execute("Demo1_CustomSource")
  }
}

class MySourceFunction extends SourceFunction[String] {

  /**
    * 向下游产生数据
    */
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random = new Random()
    while (true) {
      val num: Int = random.nextInt(100)
      ctx.collect(s"random[${num}]")
      Thread.sleep(500)
    }
  }

  /**
    * 取消，用于控制run方法结束
    */
  override def cancel(): Unit = {}
}