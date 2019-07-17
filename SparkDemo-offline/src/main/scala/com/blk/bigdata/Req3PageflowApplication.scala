package com.blk.bigdata

import com.blk.bigdata.model.UserVisitAction
import com.blk.bigdata.util.{ConfigurationUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * 页面单跳转化率统计
  */
object Req3PageflowApplication {

  def main(args: Array[String]): Unit = {

    // 准备SparkSql的环境
    val conf = new SparkConf().setAppName("Req3PageflowApplication").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    // TODO 4.1 从hive表中获取用户点击数据

    val startDate = ConfigurationUtil.getValueByJsonKey("startDate")
    val endDate = ConfigurationUtil.getValueByJsonKey("endDate")

    var sql = "select * from user_visit_action where 1=1"

    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + " and date >= '" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + " and date <= '" + endDate + "'"
    }

    spark.sql("use " + ConfigurationUtil.readFile("hive.database"))

    val dataDF: DataFrame = spark.sql(sql)
    val dataDS: Dataset[UserVisitAction] = dataDF.as[UserVisitAction]
    val dataAction: RDD[UserVisitAction] = dataDS.rdd

    spark.sparkContext.setCheckpointDir("cp")

    // ************************ 需求3 start ****************************************

    // TODO 计算分母数据
    // TODO 4.1 获取用户访问数据，进行过滤，保留需要进行统计的数据

    val sessionIdArray: Array[String] = ConfigurationUtil.getValueByJsonKey("targetPageFlow").split(",")

    val pageIdArray: Array[String] = sessionIdArray.zip(sessionIdArray.tail).map {
      case (pid1, pid2) => {
        pid1 + "-" + pid2
      }
    }


    // 1-2, 2-3,....
    val filterRDD: RDD[UserVisitAction] = dataAction.filter {
      data => {
        sessionIdArray.contains(data.page_id.toString)
      }
    }

    val pageIdToOneRDD = filterRDD.map {
      action => {
        (action.page_id, 1L)
      }
    }

    // TODO 4.2 将每一个页面的点击进行聚合，获取结果的分母数据
    val pageIdToSumRDD: RDD[(Long, Long)] = pageIdToOneRDD.reduceByKey(_ + _)

    val resultMap: Map[Long, Long] = pageIdToSumRDD.collect().toMap

    //resultMap.foreach(println)


    // TODO 计算分子数据
    // 将数据保存到检查点中
    dataAction.checkpoint()
    // TODO 4.3 获取用户访问数据，对sessionid进行分组
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = dataAction.groupBy(_.session_id)

    // TODO 4.4 对分组后的数据进行时间排序（升序）
    val sessionIdToListRDD: RDD[(String, List[(String, Long)])] = sessionGroupRDD.mapValues {
      iter => {
        val list: List[UserVisitAction] = iter.toList.sortBy(_.date)
        val pageSortList: List[Long] = list.map(_.page_id)

        // TODO 4.5 将排序后的页面数据进行拉链处理（12,23,34）
        val papone: List[(String, Long)] = pageSortList.zip(pageSortList.tail).map {
          case (pageId1, pageId2) => {
            // TODO 4.6 将拉链后的数据进行结构转换（  （12, 1）, （23, 1） ）
            (pageId1 + "-" + pageId2, 1L)
          }
        }
        papone
      }
    }

    //sessionIdToListRDD.foreach(println)

    val pageIdsToOneListRDD: RDD[List[(String, Long)]] = sessionIdToListRDD.map(_._2)

    //扁平化
    val pageIdsToOneRDD: RDD[(String, Long)] = pageIdsToOneListRDD.flatMap(d=>d)

    // 过滤，保留需要关心跳转的页面（1-2,2-3,3-4,4-5,5-6,6-7）
    val pageIdsToOneFilterRDD: RDD[(String, Long)] = pageIdsToOneRDD.filter {
      case (pageId, one) => {
        pageIdArray.contains(pageId)
      }
    }

    //pageIdsToOneFilterRDD.foreach(println)

    // TODO 4.7 将转换结构后的数据进行聚合（  （12, 10）, （23, 100） ），获取分子数据
    val pageIdsToSumRDD: RDD[(String, Long)] = pageIdsToOneFilterRDD.reduceByKey(_+_)

    // TODO 4.8 将分子数据除以分母数据，获取最终的结果
    pageIdsToSumRDD.foreach{
      case (pageid,sum) => {
        val keys = pageid.split("-")

        println(pageid +"=" + (sum.toDouble / resultMap(keys(0).toLong)))
      }
    }



    // ************************ 需求3 end ****************************************
    // 释放资源
    spark.stop()

  }
}
