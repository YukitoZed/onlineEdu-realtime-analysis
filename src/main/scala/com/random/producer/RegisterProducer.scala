package com.random.producer

import java.util.Properties

import com.random.util.PropertiesUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    ssc.textFile(this.getClass.getResource("/register.log").getPath, 10)
      .foreachPartition(partition => {
        val props: Properties = PropertiesUtil.load("config.properties")

        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("register_topic",item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
