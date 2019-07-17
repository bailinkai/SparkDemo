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
object Req3BlackNameList {
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

    val filterDStream: DStream[KafkaDemo] = consumerDStream.transform(
      rdd => {
        val jedisClient = RedisUtil.getJedisClient
        val blackList: util.Set[String] = jedisClient.smembers("blacklist")
        jedisClient.close()

        val blackListBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackList)
        rdd.filter {
          message => {
            !blackListBroadcast.value.contains(message)
          }
        }
      }
    )


    // 将过滤后的数据进行结构的转换，为了方便统计
    val dateAdsUserToOne: DStream[(String, Long)] = filterDStream.map {
      message => {
        val date: String = DateUtil.parseTimestampToString(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.adid + "_" + message.userid, 1L)
      }
    }

    //设置检查点
    ssc.sparkContext.setCheckpointDir("cp2")

    // 使用有状态的聚合操作
    val dateAdsUserToSum: DStream[(String, Long)] = dateAdsUserToOne.updateStateByKey {
      case (seq, buffer) => {
        val sum = seq.size + buffer.getOrElse(0L)
        Option(sum)
      }
    }

    // 判断有状态数据聚合结果是否超过阈值，如果超过，将用户拉入黑马单
    dateAdsUserToSum.foreachRDD{
      rdd => {
        rdd.foreach{
          case (key,sum) => {
            if(sum >= 100){
              val client = RedisUtil.getJedisClient
              client.sadd("blacklist",key.split("_")(2))

              println("key"+key)

              client.close()
            }
          }
        }
      }
    }


    //启动
    ssc.start()
    ssc.awaitTermination()

  }
}

