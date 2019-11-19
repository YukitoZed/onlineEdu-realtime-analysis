package com.random.consumer

import com.random.util.MyKafkaUtil
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

object RegisterConsumer {
  /**
    * 获取Kafka数据(已废弃）
    * @param topic
    * @param ssc
    * @return
    */
  def getRegisterLog(topic: String, ssc: StreamingContext,offsetMap:mutable.HashMap[TopicPartition, Long]) = {

      MyKafkaUtil.getKafkaStream(topic, ssc,offsetMap)

  }
}
