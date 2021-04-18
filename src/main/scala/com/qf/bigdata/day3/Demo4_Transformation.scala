package com.qf.bigdata.day3

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Demo4_Transformation {
  def main(args: Array[String]): Unit = {
    //1. 获取到flink环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 读取
    val dataStream: DataStream[String] = env.fromElements("I love you very much")
    //    val wcStream: DataStream[WordCount] = dataStream.flatMap(_.split("\\s+")).filter(_.length >= 4).map(WordCount(_, 1))
    //      .keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1))
    //      .sum("count")
    //    wcStream.print.setParallelism(1)


    /*
     * dataStream --》 SplitStream
     * split 和 select
     */
    val splitStream: SplitStream[WordCount] = dataStream.map(info => {
      val arr: Array[String] = info.split("\\s+")
      val word: String = arr(0)
      val cnt: Int = arr(1).toInt
      WordCount(word, cnt)
    }).split((wc: WordCount) => {
      if (wc.word.equals("love")) Seq("baby")
      else Seq("an")
    })

    splitStream.select("baby").print("666").setParallelism(1)
    splitStream.select("an").print("777").setParallelism(1)

    env.execute("Demo4_Transformation")
  }
}

case class WordCount(word:String, count:Int)