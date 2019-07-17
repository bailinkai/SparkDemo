package com.blk.bigdata

import java.sql.{Connection, DriverManager}
import java.util.UUID

import com.blk.bigdata.model.UserVisitAction
import com.blk.bigdata.util.{ConfigurationUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Req8PageStandTime {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Top10AM").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    spark.sql("use " + ConfigurationUtil.readFile("hive.database"))

    //val actionDF: DataFrame = spark.sql("select * from user_visit_action where 1=1")
    var sql = "select * from user_visit_action where 1=1"

    val startDate = ConfigurationUtil.getValueByJsonKey("startDate")
    val endDate = ConfigurationUtil.getValueByJsonKey("endDate")

    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + " and date >= '" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + " and date <= '" + endDate + "'"
    }

    val actionDF: DataFrame = spark.sql(sql)

    val actionRDD: RDD[UserVisitAction] = actionDF.as[UserVisitAction].rdd

    // TODO 1. 将数据根据session进行分组
    val groupByRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    // TODO 2. 将分组后的数据进行时间排序(升序)
    val sortRDD: RDD[(String, List[(Long, Long)])] = groupByRDD.mapValues {
      action => {
        val list = action.toList.sortBy(_.date)

        //转换结构
        val pageToTime: List[(Long, Long)] = list.map {
          ac => {
            val times = DateUtil.parseTimestampToLong(ac.action_time)
            (ac.page_id, times)
          }
        }

        // TODO 3. 将页面数据进行拉链((1-2),(time2-time1))
        val pageTopage: List[((Long, Long), (Long, Long))] = pageToTime.zip(pageToTime.tail)

        // TODO 4. 将拉链数据进行结构的转变(1,(timeX)),(1,(timeX)),(1,(timeX))
        val pageToTimeXList: List[(Long, Long)] = pageTopage.map {
          case ((p1, t1), (p2, t2)) => {
            (p1, t2 - t1)
          }
        }
        pageToTimeXList
      }
    }

    // TODO 5. 将转变结构后的数据进行分组(pageid, Iterator[(time)])
    val mapRDD: RDD[List[(Long, Long)]] = sortRDD.map(_._2)

    val flatMapRDD: RDD[(Long, Long)] = mapRDD.flatMap(t => t)

    val groupPageIdRDD: RDD[(Long, Iterable[Long])] = flatMapRDD.groupByKey()

    // TODO 6. 获取最终结果：(pageid, timeSum / timeSize])
    val resultRDD: RDD[(Long, Double)] = groupPageIdRDD.map {
      case (pageid, iter) => {
        (pageid, iter.sum / iter.size)
      }
    }

    resultRDD.foreach(println)

    //释放资源
    spark.stop()

  }
}