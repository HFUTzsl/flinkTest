package com.qf.bigdata.day3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Demo9_Redis_Sink {
  def main(args: Array[String]): Unit = {
    //1. 准备数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[Emp] = env.socketTextStream("146.56.208.76", 6666)
      .map(line => {
        val fields: Array[String] = line.split("\\s+")
        println(fields.mkString(","))
        Emp(fields(0).toInt, fields(1), fields(2).toDouble, fields(3))
      })
    //2. 将数据进行转换
    val redisStream: DataStream[(String, String)] = dataStream.map(emp => (emp.name, emp.address))
    redisStream.print.setParallelism(1)

    //3. 创建redisSink
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("146.56.208.76")
      .setPort(6379)
      .build()

    val sink: RedisSink[(String, String)] = new RedisSink(config, new MyRedisMapper)

    redisStream.addSink(sink)

    //4. 启动
    env.execute("Demo9_Redis_Sink")
  }
}

class MyRedisMapper extends RedisMapper[(String, String)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
}