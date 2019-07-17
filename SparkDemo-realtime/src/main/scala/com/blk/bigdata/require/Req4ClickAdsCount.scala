package com.blk.bigdata.require

import java.util

import com.blk.bigdata.util.MyKafkaUtil.MyKafkaUtil
import com.blk.bigdata.util.{DateUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 广告点击量实时统计
  */
object Req4ClickAdsCount{
  def main(args: Array[String]): Unit = {

    //获取配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req3BlackNameList")

    //获取StreamingContext对象，设置窗口大小
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //kafka的主题topic
    val topic = "ads_log"

    //使用工具类生成DS
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    //测试问题
    val consumerDStream: DStream[KafkaDemo] = kafkaDStream.map {
      datas => {
        val keys = datas.value().split(" ")
        KafkaDemo(keys(0), keys(1), keys(2), keys(3), keys(4))
      }
    }


    // 将过滤后的数据进行结构的转换，为了方便统计
    val dateAdsUserToOne: DStream[(String, Long)] = consumerDStream.map {
      message => {
        val date: String = DateUtil.parseTimestampToString(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.adid + "_" + message.userid, 1L)
      }
    }

    // 聚合操作
    val reduceDStream: DStream[(String, Long)] = dateAdsUserToOne.reduceByKey(_+_)


    // 更新redis的结果
    reduceDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          datas => {
            val client = RedisUtil.getJedisClient

            datas.foreach {
              case (key, sum) => {
                client.hincrBy("date:area:city:ads", key, sum)
              }
            }

            client.close()
          }
        }
      }
    }

    //启动
    ssc.start()
    ssc.awaitTermination()

  }
}

