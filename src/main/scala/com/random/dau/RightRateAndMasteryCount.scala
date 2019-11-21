package com.random.dau

import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.random.consumer.ConsumerUtil
import com.random.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RightRateAndMasteryCount {

  var offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

  /**
    * 对题目表进行更新操作
    *
    * @param key
    * @param itr
    * @param sqlProxy
    * @param conn
    * @return
    */

  /*
      1004-501-14       1004	    501	      14	      5	        1	          2019-07-12 11:17:45,1004	    501	      14	      5	        1	          2019-07-12 11:17:45
      1012-504-24	      1012	    504	      24	      0	        0	          2019-07-12 11:17:45,1004	    501	      14	      5	        1	          2019-07-12 11:17:45
      1012-503-13	      1012	    503	      13	      8	        0	          2019-07-12 11:17:45,1004	    501	      14	      5	        1	          2019-07-12 11:17:45
      1001-502-13	      1001	    502	      13	      2	        1	          2019-07-12 11:17:45,1004	    501	      14	      5	        1	          2019-07-12 11:17:45
   */

  def qzQuestionUpdate(key: String, itr: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, conn: Connection) = {

    val keys: Array[String] = key.split("-")
    val userid = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt

    //本批次所有做过的题目：[(1004,501,14,5,1,2019-07-12 11:17:45),(1004,501,14,5,1,2019-07-12 11:17:45)]
    val array = itr.toArray

    //[1,2,3,4,5,6]
    val questionids = array.map(_._4).distinct //对当前批次的数据下questionid 去重 同一个用户的做过的题的id

    //查询历史数据下的 questionid
    var questionids_history: Array[String] = Array()

    sqlProxy.executeQuery(conn, "select questionids from qz_point_history where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            questionids_history = rs.getString(1).split(",")
            println("从数据库查出来的数据：" + rs.toString)
          }
          rs.close()
        }
      })

    val resultQuestionid: Array[String] = questionids.union(questionids_history).distinct //同一用户做过的所有题目id
    val countSize: Int = resultQuestionid.length // 同一用户做过的所有的题数
    val resultQuestionid_str: String = resultQuestionid.mkString(",")
    val qz_count: Int = questionids.length //当前做过的题数
    var qz_sum: Int = array.length //当前批次题目总数
    var qz_istrue = array.filter(_._5.equals("1")).size //当前批次正确的题数
    val createtime: String = array.map(_._6).min //最早创建的时间作为表中创建时间
    val updatetime: String = DateTimeFormatter.ofPattern("yyyy-MM-dd- HH:mm:ss").format(LocalDateTime.now())

    //更新qz_point_history表，用于存放同一用户做过的所有题目
    sqlProxy.executeUpdate(conn, "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime)" +
      " values(?,?,?,?,?,?) on duplicate key update questionids=?,updatetime=?", Array(userid, courseid, pointid,
      resultQuestionid_str, createtime, createtime, resultQuestionid_str, updatetime))

    var qzSum_history = 0
    var istrue_history = 0
    sqlProxy.executeQuery(conn, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid),
      new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            qzSum_history += rs.getInt(1)
            istrue_history += rs.getInt(2)
          }
          rs.close()
        }
      }
    )
    qz_sum += qzSum_history
    qz_istrue += istrue_history

    val correct_rate: Double = qz_istrue.toDouble / qz_sum.toDouble //计算正确率

    val qz_detail_rate: Double = countSize.toDouble / 30
    val mastery_rate: Double = qz_detail_rate * correct_rate

    sqlProxy.executeUpdate(conn, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("register_count")

    //开启背压机制
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //由于Kafka数据积压，设置参数防止OOM
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

    val ssc = new StreamingContext(conf, Seconds(3))

    val sc: SparkContext = ssc.sparkContext

    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    // 连接数据库
    //    val conn: Connection = ConnectionPool.getinstance().getConnection

    //查询mysql中是否存在偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array("qz_point_group"), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = ConsumerUtil.getQzLog("qz_log", ssc, offsetMap)

    //过滤不正常数据并且转换格式
    val dsStream: DStream[(String, String, String, String, String, String)] = inputDStream.filter(item => item.value().split("\t").length == 6)
      .mapPartitions(partition => {
        partition.map(item => {
          val line: String = item.value()
          val arr: Array[String] = line.split("\t")
          val uid = arr(0) //用户id
          val courseid = arr(1) //课程id
          val pointid = arr(2) //知识点id
          val questionid = arr(3) //题目id
          val istrue = arr(4) //是否正确
          val createtime = arr(5) //创建时间
          (uid, courseid, pointid, questionid, istrue, createtime)
        })
      })
    dsStream
      .foreachRDD(rdd => {

        //获取同一用户同一课程同一知识点的数据
        val groupRDD = rdd.groupBy(item => item._1 + "-" + item._2 + "-" + item._3)

        /*
            1004-501-14       1004	    501	      14	      5	        1	          2019-07-12 11:17:45
            1012-504-24	      1012	    504	      24	      0	        0	          2019-07-12 11:17:45
            1012-503-13	      1012	    503	      13	      8	        0	          2019-07-12 11:17:45
            1001-502-13	      1001	    502	      13	      2	        1	          2019-07-12 11:17:45
         */


        groupRDD.foreachPartition(partition => {
          //在分区下获取jdbc连接
          val sqlProxy: SqlProxy = new SqlProxy
          val conn: Connection = DataSourceUtil.getConnection
          try {
            partition.foreach {
              case (key, itr) => {
                qzQuestionUpdate(key, itr, sqlProxy, conn)
              }
            }
          } catch {
            case e:Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(conn)
          }
        })
      })

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
    /*val resultWithoutAnswer = inputDStream
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
    masteryRate.print()*/

    //    result.print()

    // 逻辑处理完毕，偏移量更新
    inputDStream.foreachRDD(
      rdd => {
        val sqlProxy: SqlProxy = new SqlProxy
        //        val conn: Connection = ConnectionPool.getinstance().getConnection
        val conn: Connection = DataSourceUtil.getConnection
        try {
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (offset <- offsetRanges) {
            sqlProxy.executeUpdate(conn, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
              Array("qz_point_group", offset.topic, offset.partition.toString(), offset.untilOffset))
          }
        } catch {
          case e:Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(conn)
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
