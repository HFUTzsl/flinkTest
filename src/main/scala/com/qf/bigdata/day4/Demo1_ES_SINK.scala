//package com.qf.bigdata.day4
//
//import java.util
//
//import com.qf.bigdata.day3.Emp
//import org.apache.flink.api.common.functions.RuntimeContext
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.apache.http.HttpHost
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//
//import scala.collection.JavaConversions
//
//
//object Demo1_ES_SINK {
//  def main(args: Array[String]): Unit = {
//    //1. 准备数据
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val dataStream: DataStream[Emp] = env.socketTextStream("146.56.208.76", 6666)
//      .map(line => {
//        val fields: Array[String] = line.split("\\s+")
//        println(fields.mkString(","))
//        Emp(fields(0).toInt, fields(1), fields(2).toDouble, fields(3))
//      })
//
//    dataStream.print.setParallelism(1)
//
//    //2. 写入es
//    val httpHosts = new java.util.ArrayList[HttpHost]
//    httpHosts.add(new HttpHost("146.56.208.76", 9200, "http"))
//
//
//    val sink: ElasticsearchSink[Emp] = new ElasticsearchSink.Builder[Emp](httpHosts, new MyElasticsearchSinkFunction()).build()
//    dataStream.addSink(sink)
//    env.execute("Demo1_ES_SINK")
//  }
//
//}
//
//class MyElasticsearchSinkFunction extends ElasticsearchSinkFunction[Emp] {
//  /**
//    * 当前数据流中，没有一个元素就调用一次这个方法
//    */
//  override def process(emp: Emp, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//    //1. 将emp导入到map
//    println(s"${emp}")
//    val map = new util.HashMap[String,String]()
//    map.put("name", emp.name)
//    map.put("address", emp.address)
//
//    //2. 创建向es发送请求的对象
//    val request: IndexRequest = Requests.indexRequest()
//      .index("fink") // 索引库
//      .`type`("info") // 类型
//      .id(s"${emp.id}") // docid
//      .source(map) // 数据
//
//    //3. 将索引请求传递给请求的索引器
//    requestIndexer.add(request)
//  }
//}
