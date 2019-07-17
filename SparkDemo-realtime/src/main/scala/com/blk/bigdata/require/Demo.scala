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
  * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。
  * 注：黑名单保存到redis中。
  * 已加入黑名单的用户不再进行检查。
  */
object Demo {
  def main(args: Array[String]): Unit = {
    val option: Option[Null] = Option(null)

    println(option.toString)
  }
}


case class KafkaDemo(timestamp: String, area: String, city: String, userid: String, adid: String)
