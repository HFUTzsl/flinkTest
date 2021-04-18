package com.qf.bigdata.day3

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer

object Demo6_Kafka_Sink {
  def main(args: Array[String]): Unit = {
    //1. 准备
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dStream: DataStream[String] = env.fromElements("lixi", "lixi2")

    //2. 获取KafkaSink
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "146.56.208.76:9092")
    properties.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
    properties.setProperty("value.serializer" , classOf[ByteArraySerializer].getName)

    val sink = new FlinkKafkaProducer[String](
      "flink",                  // target topic
      new SimpleStringSchema(),    // serialization schema
      properties                  // producer config
    ) // fault-tolerance

    //3.将数据流sink到kafka
    dStream.addSink(sink)

    env.execute("Demo6_Sink")
  }
}