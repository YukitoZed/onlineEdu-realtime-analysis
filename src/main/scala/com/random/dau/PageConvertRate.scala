package com.random.dau

import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSONObject
import com.random.consumer.ConsumerUtil
import com.random.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object PageConvertRate {

  /**
    * 计算转换率
    *
    * @param sqlProxy
    * @param conn
    */
  def calJumRate(sqlProxy: SqlProxy, conn: Connection) = {

    var page1_num = 0L
    var page2_num = 0L
    var page3_num = 0L

    // 1页面的数量
    sqlProxy.executeQuery(conn, "select num from page_jump_rate where page_id=?",
      Array(1), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            page1_num = rs.getLong(1)
          }
        }
      })

    // 2页面的数量
    sqlProxy.executeQuery(conn, "select num from page_jump_rate where page_id=?",
      Array(2), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            page1_num = rs.getLong(1)
          }
        }
      })

    // 3页面的数量
    sqlProxy.executeQuery(conn, "select num from page_jump_rate where page_id=?",
      Array(3), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            page1_num = rs.getLong(1)
          }
        }
      })

    val nf: NumberFormat = NumberFormat.getPercentInstance()
    val page1Topage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2Topage3Rate = if (page1_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    sqlProxy.executeUpdate(conn, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1Topage2Rate, 2))
    sqlProxy.executeUpdate(conn, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2Topage3Rate, 3))
  }

  /**
    * 计算页面跳转个数
    *
    * @param sqlProxy
    * @param item
    * @param conn
    */
  def calPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), conn: Connection) = {

    val keys: Array[String] = item._1.split("_")
    var num: Long = item._2
    val page_id = keys(1).toInt //获取当前page_id
    val last_page_id = keys(0).toInt //获取上一page_id
    val next_page_id = keys(2).toInt //获取下页面page_id
    sqlProxy.executeQuery(conn, "select num from page_jump_rate where page_id=?",
      Array(page_id), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            num += rs.getLong(1)
          }
          rs.close()
        }
      })

    //对num 进行修改 并且判断当前page_id是否为首页
    if (page_id == 1) {
      sqlProxy.executeUpdate(conn, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)" +
        "values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, "100%", num))
    } else {
      sqlProxy.executeUpdate(conn, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num)" +
        "values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, num))
    }
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("page_convert")

    // 背压 最大拉取数据 优雅关闭
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val sc: SparkContext = ssc.sparkContext

    //高可用
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    val sqlProxy: SqlProxy = new SqlProxy
    val conn: Connection = DataSourceUtil.getConnection

    val offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

    // 查偏移量
    try {
      sqlProxy.executeQuery(conn, "select * from offset_manager where groupid=?", Array("page_groupid"),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model: TopicPartition = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset: Long = rs.getLong(4)
              offsetMap.put(model, offset)
            }
          }
        }
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(conn)
    }

    val inputDStream = ConsumerUtil.getPageLog("page_topic", ssc, offsetMap)

    val dsStream = inputDStream
      .filter(
        item => {
          val line: String = item.value()
          val jSONObject: JSONObject = ParseJsonData.parseObject(line)
          jSONObject.isInstanceOf[JSONObject]
        })
      .map(item => item.value())
      .mapPartitions(partition => {
        partition.map(
          item => {
            val jsonObject: JSONObject = ParseJsonData.parseObject(item)
            val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
            val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
            val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
            val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
            val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
            val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
            val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
            (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
          }
        )
      })

    dsStream.cache()

    val pageValueDStream = dsStream.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))
    val resultDStream = pageValueDStream.reduceByKey(_ + _)

    resultDStream.foreachRDD(
      rdd => {
        val sqlProxy: SqlProxy = new SqlProxy
        val conn: Connection = DataSourceUtil.getConnection
        rdd.foreachPartition(
          partition => {
            partition.foreach(
              item => {
                calPageJumpCount(sqlProxy, item, conn)
              }
            )
          }
        )
      }
    )


    // 业务逻辑处理完写入MySQL偏移量
    inputDStream.foreachRDD(
      rdd => {
        val sqlProxy: SqlProxy = new SqlProxy
        val conn: Connection = DataSourceUtil.getConnection
        try {
          calJumRate(sqlProxy, conn)
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (offset <- offsetRanges) {
            sqlProxy.executeUpdate(conn, "replace into `offset_manager` (groupid,topic,'partition',untilOffset) values (?,?,?,?)",
              Array("page_groupid", offset.topic, offset.partition.toString, offset.untilOffset))
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(conn)
        }

      }
    )


  }
}
