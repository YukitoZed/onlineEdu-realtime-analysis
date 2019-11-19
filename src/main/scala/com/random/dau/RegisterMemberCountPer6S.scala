package com.random.dau

import com.random.consumer.RegisterConsumer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RegisterMemberCountPer6S {
  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("register_count"), Seconds(3))

    val inputDStream = RegisterConsumer.getRegisterLog("register_topic", ssc)

    val result = inputDStream.map {
      record => {
        val line: String = record.value()
        val fileds: Array[String] = line.split("\t")
        (fileds(1), 1)
      }
    }
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6))

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
