package com.qf.bigdata.day3
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

object Demo5_Union_Connect {
  def main(args: Array[String]): Unit = {
    /**
      * union 和 connnect
      * union ： dataStream* --> dataStream
      * connect : * --> connectStream
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream1: DataStream[String] = env.socketTextStream("146.56.208.76", 6666)
    val dataStream2: DataStream[String] = env.fromElements("lixi 1")

    val wcStream2: DataStream[WordCount] = dataStream2.map(info => {
      val arr: Array[String] = info.split("\\s+")
      val word: String = arr(0)
      val cnt: Int = arr(1).toInt
      WordCount(word, cnt)
    })

    val wcStream1: DataStream[WordCount] = dataStream1.map(info => {
      val arr: Array[String] = info.split("\\s+")
      val word: String = arr(0)
      val cnt: Int = arr(1).toInt
      WordCount(word, cnt)
    })

    //    val wordcountStream: DataStream[WordCount] = wcStream1.union(wcStream2)
    //    wordcountStream.print().setParallelism(1)

    val connectStream: ConnectedStreams[WordCount, WordCount] = wcStream1.connect(wcStream1)

    env.execute("wc")

//    val cnt: DataStream[WordCount] = data.flatMap(_.split("\\s+")).map(WordCount(_, 1L)).keyBy("word")
//      .timeWindow(Time.seconds(2), Time.seconds(1)).sum("count")
    //      .reduce((a,b) => WordCount(a.word, a.count + b.count))
  }
}