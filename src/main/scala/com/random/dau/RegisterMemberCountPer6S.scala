package com.random.dau

import java.sql.{Connection, ResultSet}

import com.random.consumer.ConsumerUtil
import com.random.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RegisterMemberCountPer6S {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("register_count")

    val ssc = new StreamingContext(conf, Seconds(3))

    //开启背压机制
    //    conf.set("spark.streaming.backpressure.enabled", "true")
    //由于Kafka数据积压，设置参数防止OOM
    //    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

    val sc: SparkContext = ssc.sparkContext
    //    val inputDStream = RegisterConsumer.getRegisterLog("register_topic", ssc)

    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    // 查询MySQL中是否有偏移量
    val sqlProxy: SqlProxy = new SqlProxy()
    val offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

    //连接数据库
    val client: Connection = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?",
        Array("register_group_test"),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              //String topic, int partition
              val model: TopicPartition = new TopicPartition(rs.getString(2), rs.getInt(2))
              //String offset
              val offset: Long = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            rs.close()
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //    val inputDStream = RegisterConsumer.getRegisterLog("register_topic", ssc)
//    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("register_topic", ssc, offsetMap)
val inputDStream: InputDStream[ConsumerRecord[String, String]] = ConsumerUtil.getRegisterLog("register_topic",ssc,offsetMap)
    // 每一行数据进行过滤查看是否是完整的日志信息
    val result: DStream[(String, Int)] = inputDStream.filter(item => {
      item.value().split("\t").length == 3
    })
      .mapPartitions(
        partitions => {
          partitions.map(
            item => {
              val line: String = item.value()
              val arr: Array[String] = line.split("\t")
              //匹配不同渠道
              val app_name: String = arr(1) match {
                case "1" => "PC"
                case "2" => "APP"
                case _ => "Other"
              }
              (app_name, 1)
            }
          )
        }
      )
    // 缓存结果
    result
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6))

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
