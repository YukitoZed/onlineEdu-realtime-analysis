package com.random.consumer

import com.random.util.MyKafkaUtil
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

object ConsumerUtil {

  /**
    * 获取Kafka注册数据
    *
    * @param topic
    * @param ssc
    * @return
    */
  def getRegisterLog(topic: String, ssc: StreamingContext, offsetMap: mutable.HashMap[TopicPartition, Long]) = {

    MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap)

  }

  /**
    * 获取Kafka做题数据
    *
    * @param topic
    * @param ssc
    * @param offsetMap
    * @return
    */
  def getQzLog(topic: String, ssc: StreamingContext, offsetMap: mutable.HashMap[TopicPartition, Long]) = {

    MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap)

  }

  /**
    * 获取Kafka页面跳转数据
    *
    * @param topic
    * @param ssc
    * @param offsetMap
    * @return
    */
  def getPageLog(topic: String, ssc: StreamingContext, offsetMap: mutable.HashMap[TopicPartition, Long]) = {

    MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap)

  }

  def getCourseLearnLog(topic: String, ssc: StreamingContext, offsetMap: mutable.HashMap[TopicPartition, Long]) = {

    MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap)

  }
}
