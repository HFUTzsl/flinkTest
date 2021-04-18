package com.qf.bigdata.day2

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

object Demo2_Kafka_Source {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink", new SimpleStringSchema(),properties))
    kafkaDataStream.print("kkkkk----->").setParallelism(1)
    env.execute("kafka source")
  }
}