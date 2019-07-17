package com.blk.bigdata.require

import com.blk.bigdata.util.MyKafkaUtil.MyKafkaUtil
import com.blk.bigdata.util.{DateUtil, RedisUtil, StringUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods

/**
  *   // 需求七：最近一分钟广告点击趋势（每10秒）
  */
object Req7ADSClick{
  def main(args: Array[String]): Unit = {

    //获取配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req3BlackNameList")

    //获取StreamingContext对象，设置窗口大小
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // TODO 设置检查点目录，因为有状态聚合
    ssc.sparkContext.setCheckpointDir("cp4")

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

    // TODO 1. 使用窗口函数将数据进行封装
    val windowDStream: DStream[KafkaDemo] = consumerDStream.window(Seconds(60),Seconds(10))

    // TODO 2. 将数据进行结构的转换 （ 15：11 ==> 15:10 , 15:25 ==> 15:20 ）
    val timeToOneDStream: DStream[(String, Long)] = windowDStream.map {
      action => {
        val str = DateUtil.parseTimestamp(action.timestamp)
        val time: String = str.substring(0, str.length - 1) + "0"
        (time, 1L)
      }
    }

    // TODO 3. 将转换结构后的数据进行聚合统计
    val reduceDStream: DStream[(String, Long)] = timeToOneDStream.reduceByKey(_+_)

    // TODO 4. 对统计结果进行排序
    val sortDStream = reduceDStream.transform(rdd => {
      rdd.sortByKey()
    })

    sortDStream.print()





    //启动
    ssc.start()
    ssc.awaitTermination()

  }
}

