package com.random.dau

import java.sql.{Connection, ResultSet}

import com.random.consumer.RegisterConsumer
import com.random.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RegisterMemberCountPer3S {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("register_count")

    //开启背压机制
    conf.set("spark.streaming.backpressure.enabled", "true")
    //由于Kafka数据积压，设置参数防止OOM
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

    val ssc = new StreamingContext(conf, Seconds(3))

    val sc: SparkContext = ssc.sparkContext
//    val inputDStream = RegisterConsumer.getRegisterLog("register_topic", ssc)

    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    ssc.checkpoint("/config/checkpoint")

    /*
    用户id     平台id  1:PC  2:APP   3:Ohter       创建时间
    0	            1	                        2019-07-16 16:01:55
    1	            1	                        2019-07-16 16:01:55
    2	            1	                        2019-07-16 16:01:55
     */

    /*val result = inputDStream.map {
      record => {
        val line: String = record.value()
        val fileds: Array[String] = line.split("\t")
        (fileds(1), 1)
      }
    }*/

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

    //查看维护偏移量的map是否有数据
    /*val inputDStream: InputDStream[ConsumerRecord[String, String]] = if(offsetMap.isEmpty) {
      KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array("register_topic"),kafkaParam))
    }else{
      KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array("register_topic"),kafkaParam,offsetMap))
    }*/

//    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("register_topic",ssc,offsetMap)

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = RegisterConsumer.getRegisterLog("register_topic",ssc,offsetMap)
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
    result.cache()

    val state: DStream[(String, Int)] = result.updateStateByKey(updateFunction)

    state.print()

    // 处理完业务逻辑 手动提交offset维护到本地MySQL中
    inputDStream.foreachRDD(rdd=>{
      val proxy: SqlProxy = new SqlProxy()
      val client: Connection = DataSourceUtil.getConnection
      try{
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offset <- offsetRanges) {
          proxy.executeUpdate(client,"replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array("register_group_test",offset.topic,offset.partition,offset.toString(),offset.untilOffset))
        }
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        sqlProxy.shutdown(client)
      }

    })
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

}
