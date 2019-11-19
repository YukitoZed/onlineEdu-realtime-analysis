package com.random.consumer

import com.random.util.MyKafkaUtil
import org.apache.spark.streaming.StreamingContext

object RegisterConsumer {

  def getRegisterLog(topic: String, ssc: StreamingContext) = {

    MyKafkaUtil.getKafkaStream(topic, ssc)

  }
}
