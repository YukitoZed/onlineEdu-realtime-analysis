package com.random.dau

import java.sql.{Connection, ResultSet}

import com.random.consumer.ConsumerUtil
import com.random.util.{ConnectionPool, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RightRateAndMasteryCount {

  var offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("register_count")

    //开启背压机制
    conf.set("spark.streaming.backpressure.enabled", "true")

    //由于Kafka数据积压，设置参数防止OOM
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

    val ssc = new StreamingContext(conf, Seconds(3))

    val sc: SparkContext = ssc.sparkContext

    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    ssc.checkpoint("/config/checkpoint")

    // 连接数据库
    val conn: Connection = ConnectionPool.getinstance().getConnection

    //操作数据库代理
    val sqlProxy: SqlProxy = new SqlProxy

    // 维护偏移量
    try {
      sqlProxy.executeQuery(conn, "select * from `offset_manager` where group id=?", Array("log_group_test"),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model: TopicPartition = new TopicPartition(rs.getString(2), rs.getInt(2))
              val offset: Long = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            rs.close()
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(conn)
    }

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = ConsumerUtil.getQzLog("qz_log", ssc, offsetMap)

    /*
    (用户id) (课程id) (知识点id) (题目id) (是否正确 0错误 1正确)(创建时间)
    1004	    501	      14	      5	        1	          2019-07-12 11:17:45
    1012	    504	      24	      0	        0	          2019-07-12 11:17:45
    1012	    503	      13	      8	        0	          2019-07-12 11:17:45
    1001	    502	      13	      2	        1	          2019-07-12 11:17:45
     */

    /*
   1004	    501	      14	      5	 ,         1
   1012	    504	      24	      0	 ,         1
   1012	    503	      13	      8	 ,         1
   1001	    502	      13	      2	 ,         1
    */
    val resultWithoutAnswer = inputDStream
      .map(record => {
        val fileds: Array[String] = record.value().split("\t")
        ((fileds(0), fileds(1), fileds(2), fileds(3)), fileds(5))
      })
      .reduceByKey((time1, time2) => { //// 相同去重关键字的数据，对时间字段进行比较，保留时间最大的
        if (time1 >= time2) {
          time1
        } else {
          time2
        }
      })
      .map {
        case (key: (String, String, String, String), value: String) => {
          (key, 1)
        }
      }

    /*
   1004	    501	      14	      5	 1,         1
   1012	    504	      24	      0	 0,         1
   1012	    503	      13	      8	 0,         1
   1001	    502	      13	      2	 1,         1
    */
    val resultWithAnswer = inputDStream
      .map(record => {
        val fileds: Array[String] = record.value().split("\t")
        ((fileds(0), fileds(1), fileds(2), fileds(3), fileds(4)), fileds(5))
      })
      .reduceByKey((time1, time2) => { //// 相同去重关键字的数据，对时间字段进行比较，保留时间最大的
        if (time1 >= time2) {
          time1
        } else {
          time2
        }
      })
      .map {
        case (key: (String, String, String, String, String), value: String) => {
          ((key._1, key._2, key._3, key._5), 1)
        }
      }


    val totalCount = resultWithoutAnswer
      .map { case (key: (String, String, String, String), value: Int) => {
        ((key._1, key._2, key._3), 1)
      }
      }
      .reduceByKey(_ + _) //同一个用户同一个课程同一知识点的做题总个数

    //1004	    501	      14 1, 1
    //1004	    501	      14 1, 1

    val rightCount = resultWithAnswer.reduceByKey(_ + _)
      .map { case (key: (String, String, String, String), value: Int) => {
        ((key._1, key._2, key._3), 1)
      }
      } //同一个用户同一课程同一知识点正确的个数

    // 知识点正确率
    val rightRate = rightCount.join(totalCount)
      .map {
        case ((key: (String, String, String), value: (Int, Int))) => {
          (key._1, (value._1 / value._2).toDouble)
        }
      }
    println("正确率。。。")
    rightRate.print()

    // 知识点掌握度

    val masteryRate: DStream[(String, Double)] = totalCount.map {
      case ((key: (String, String, String), value: Int)) => {
        (key._1, (value / 30).toDouble)
      }
    }
      .join(rightRate)
      .map {
        case ((userid, (doneCount, right))) => {
          (userid, (doneCount * right))
        }
      }
    println("掌握度。。。")
    masteryRate.print()

    //    result.print()

    // 逻辑处理完毕，偏移量更新
    inputDStream.foreachRDD(
      rdd => {
        val sqlProxy: SqlProxy = new SqlProxy
        val conn: Connection = ConnectionPool.getinstance().getConnection
        try {
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (offset <- offsetRanges) {
            sqlProxy.executeUpdate(conn, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
              Array("log_group_test", offset.topic, offset.partition, offset.toString(), offset.untilOffset))
          }
        } catch {
          case e => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(conn)
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
