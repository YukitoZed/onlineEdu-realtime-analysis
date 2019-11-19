package com.random.dau

import com.random.consumer.RegisterConsumer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RegisterMemberCountPer3S {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("register_count")

    conf.set("spark.streaming.backpressure.enabled","true")
    conf.set("spark.streaming.kafka.maxRatePerPartition","1000")

    val ssc = new StreamingContext(conf, Seconds(3))

//    val sc: SparkContext = ssc.sparkContext
    val inputDStream = RegisterConsumer.getRegisterLog("register_topic", ssc)

//    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
//    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    ssc.checkpoint("/config/checkpoint")

    /*
    用户id     平台id  1:PC  2:APP   3:Ohter       创建时间
    0	            1	                        2019-07-16 16:01:55
    1	            1	                        2019-07-16 16:01:55
    2	            1	                        2019-07-16 16:01:55
     */

    val result = inputDStream.map {
      record => {
        val line: String = record.value()
        val fileds: Array[String] = line.split("\t")
        (fileds(1), 1)
      }
    }

    val state: DStream[(String, Int)] = result.updateStateByKey(updateFunction)

    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

}
