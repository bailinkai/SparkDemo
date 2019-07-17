package com.blk.bigdata.require

import com.blk.bigdata.util.MyKafkaUtil.MyKafkaUtil
import com.blk.bigdata.util.{DateUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods

/**
  *   // 需求六：每天各地区 top3 热门广告
  */
object Req6DateAreaAdsClickCountTop3{
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


    // 将过滤后的数据进行结构的转换，为了方便统计
    val dateAdsUserToOne: DStream[(String, Long)] = consumerDStream.map {
      message => {
        val date: String = DateUtil.parseTimestampToString(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.area + "_" + message.adid, 1L)
      }
    }

    // TODO 2. 将转换后的结构后的数据进行有状态聚合 ，需要设置检查点目录
    val updateReduceDStream: DStream[(String, Long)] = dateAdsUserToOne.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }


    // TODO 3. 将聚合后的结果进行结构的转换（date-area-ads, sum）,（date-area-ads, sum）
    // TODO 4. 将转换结构后的数据进行聚合（date-area-ads, totalSum）

    // TODO 5. 将聚合后的结果进行结构的转换（date-area-ads, totalSum）==> （date-area, (ads, totalSum)）
    val mapDStream: DStream[(String, (String, Long))] = updateReduceDStream.map {
      case (daa, sum) => {
        val keys = daa.split("_")
        (keys(0) + "_" + keys(1), (keys(2), sum))
      }
    }

    // TODO 6. 将数据进行分组
    val groupDateAndAreaDStream: DStream[(String, Iterable[(String, Long)])] = mapDStream.groupByKey()


    // TODO 7. 对分组后的数据排序（降序），取前三
    val top3DStream: DStream[(String, Map[String, Long])] = groupDateAndAreaDStream.mapValues {
      iter => {
        val tuples: List[(String, Long)] = iter.toList.sortBy(_._2).reverse.take(3)
        tuples.toMap
      }
    }

    /*top3DStream.foreachRDD{
      rdd => {
        rdd.foreach{
          case (key,v) => {
            for(i <- v){
              println("时间 地区 "+key + "    广告" +i._1+"    次数"+i._2)
            }
          }
        }
      }
    }*/



    // 更新redis的结果
    top3DStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          datas => {
            val client = RedisUtil.getJedisClient

            datas.foreach{
              case (key,map) => {

                val keys = key.split("_")

                import org.json4s.JsonDSL._
                val v = JsonMethods.compact(JsonMethods.render(map))
                client.hset("top3_ads_per_day:"+keys(0),keys(1),v)
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

