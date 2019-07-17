package com.blk.bigdata

import java.sql.{Connection, DriverManager}
import java.util.UUID

import com.blk.bigdata.model.UserVisitAction
import com.blk.bigdata.util.{ConfigurationUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Top10 热门品类中 Top10 活跃 Session 统计
  */
object Top10AMClickTop10 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Top10AMClickTop10").setMaster("local[*]")

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

    //println(actionDF.count())

    //自定义累加器
    val accumulator: CategoryAccumulator = new CategoryAccumulator

    //注册累加器
    spark.sparkContext.register(accumulator, "Category")

    //使用累加器
    actionRDD.foreach {
      action => {
        if (action.click_category_id != -1) {
          accumulator.add(action.click_category_id + "-click")
        } else if (StringUtil.isNotEmpty(action.order_category_ids)) {

          val ids = action.order_category_ids.split(",")

          for (id <- ids) {
            accumulator.add(id + "-order")
          }
        } else if (StringUtil.isNotEmpty(action.pay_category_ids)) {

          val ids = action.pay_category_ids.split(",")

          for (id <- ids) {
            accumulator.add(id + "-pay")
          }
        }
      }
    }

    //println(accumulator.value)

    //数据格式  (品类id-指标,sum)
    val accData: mutable.HashMap[String, Long] = accumulator.value

    //数据格式  (品类id-指标,sum) =》(品类id，[(order,sum),(click,sum),(pay,sum)])
    val groupRDD: Map[String, mutable.HashMap[String, Long]] = accData.groupBy {
      case (key, sum) => {
        val strings = key.split("-")
        strings(0)
      }
    }

    val taskId = UUID.randomUUID().toString
    //将分组后的数据转换为对象 CategoryTop10(categoryid, click,order,pay )
    val categoryTop10Datas = groupRDD.map {
      case (categoryid, map) => {
        CategoryTop10(taskId, categoryid, map.getOrElse(categoryid + "-click", 0L), map.getOrElse(categoryid + "-order", 0L), map.getOrElse(categoryid + "-pay", 0L))
      }
    }

    //排序  将转换后的对象进行排序（点击，下单， 支付）（降序）,需要转化为List
    val sortList: List[CategoryTop10] = categoryTop10Datas.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }

    //将排序后的结果取前10名保存到Mysql数据库中
    val top1s = sortList.take(10)

    val top10List: List[String] = top1s.map(_.categoryId)

    //*****************************************需求二代码****************************************//

    //过滤数据
    val filterRDD: RDD[UserVisitAction] = actionRDD.filter {
      data => {
        if (data.click_category_id != -1) {
          top10List.contains(data.click_category_id.toString)
        } else {
          false
        }
      }
    }

    //将筛选过滤的数据进行结构的转换（categoryid, sessionid, click）( categoryid-sessionid,1 )
    val categoryAndSeeeionToOneRDD: RDD[(String, Int)] = filterRDD.map {
      data => {
        (data.click_category_id + "_" + data.session_id, 1)
      }
    }

    //分组聚合数据  ( categoryid-sessionid,1 ) => ( categoryid-sessionid, sum)
    val categoryAndSeeeionToSumRDD = categoryAndSeeeionToOneRDD.reduceByKey(_ + _)

    //分组    ( categoryid-sessionid, sum) => ( categoryid,Iterator[(sessionid, sum)])
    val mapRDD = categoryAndSeeeionToSumRDD.map {
      case (categoryid_sessionid, sum) => {
        val ks = categoryid_sessionid.split("_")
        (ks(0), (ks(1), sum))
      }
    }

    val groupKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    //groupKeyRDD.s

    //将分组后的数据进行排序（降序）
    val sortRDD: RDD[(String, List[(String, Int)])] = groupKeyRDD.mapValues {
      datas => {
        datas.toList.sortBy(_._2).reverse
      }.take(10)
    }

    /*val sortRDD: RDD[(String, List[(String, Int)])] = groupKeyRDD.map {
      datas => {
        val dl: List[(String, Int)] = datas._2.toList

        val res = dl.sortBy(_._2).reverse.take(10)

        (datas._1, res)

      }
    }*/



    //将数据存入对象中    ( categoryid,List[(sessionid, sum)])
    val objList: RDD[List[CategoryTop10SessionTop10]] = sortRDD.map {
      case (categoryid, list) => {
        list.map {
          case (sessionid, sum) => CategoryTop10SessionTop10(taskId, categoryid, sessionid, sum)
        }
      }
    }

    //扁平化处理
    val categoryRDD: RDD[CategoryTop10SessionTop10] = objList.flatMap(list => list)


    //*****************************************需求二代码****************************************//


    //测试查询结果
    //println(top1s)

    categoryRDD.foreachPartition {
      datas => {

        val driverClass = ConfigurationUtil.readFile("jdbc.driver.class")
        val url = ConfigurationUtil.readFile("jdbc.url")
        val user = ConfigurationUtil.readFile("jdbc.user")
        val passwd = ConfigurationUtil.readFile("jdbc.password")

        //注册驱动
        Class.forName(driverClass)

        val connection: Connection = DriverManager.getConnection(url, user, passwd)


        val insertSQL = "insert into category_top10_session_count values ( ?, ?, ?, ? )"

        val preparedStatement = connection.prepareStatement(insertSQL)

        datas.foreach {
          data => {
            preparedStatement.setString(1, data.taskId)
            preparedStatement.setString(2, data.categoryId)
            preparedStatement.setString(3, data.sessionId)
            preparedStatement.setLong(4, data.clickCount)
            preparedStatement.executeUpdate()
          }
        }

        //释放资源
        preparedStatement.close()
        connection.close()

      }
    }


    spark.stop()
  }
}

case class CategoryTop10SessionTop10(taskId: String, categoryId: String, sessionId: String, clickCount: Long)