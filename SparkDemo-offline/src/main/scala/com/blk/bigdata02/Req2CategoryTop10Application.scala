package com.blk.bigdata02

import java.sql.{Connection, DriverManager}
import java.util.UUID

import com.blk.bigdata.model.UserVisitAction
import com.blk.bigdata.util.{ConfigurationUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req2CategoryTop10Application {
  def main(args: Array[String]): Unit = {

    // 准备SparkSql的环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //导入spark类环境
    import spark.implicits._

    // TODO 4.1 从hive表中获取用户点击数据
    spark.sql("use " + ConfigurationUtil.readFile("hive.database"))

    //获取开始日期和结束日期
    val startDate = ConfigurationUtil.getValueByJsonKey("startDate")
    val endDate = ConfigurationUtil.getValueByJsonKey("endDate")

    var sql = "select * from user_visit_action where 1=1"

    //拼接条件
    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + " and date >= '" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + " and date <= '" + endDate + "'"
    }

    //执行sql
    val dataDF: DataFrame = spark.sql(sql)

    //转换结构
    val dataDS: Dataset[UserVisitAction] = dataDF.as[UserVisitAction]
    val dataRDD: RDD[UserVisitAction] = dataDS.rdd

    //println(dataRDD.count())

    // TODO 4.2 声明累加器，并使用累加器来聚合数据

    val categoryCountAccumulatorV = new CategoryCountAccumulatorV2
    // 注册累加器
    spark.sparkContext.register(categoryCountAccumulatorV, "CategoryCountAccumulatorV2")

    // 获取累加器的结果
    // (1_click, 10), (1_order, 20)
    dataRDD.foreach {
      data => {
        //判断对象的属性值
        if (data.click_category_id != -1) {

          categoryCountAccumulatorV.add(data.click_category_id + "-click")

        } else if (StringUtil.isNotEmpty(data.order_category_ids)) {

          val ids: Array[String] = data.order_category_ids.split(",")
          for (id <- ids) {
            categoryCountAccumulatorV.add(id + "-order")
          }
        } else if (StringUtil.isNotEmpty(data.pay_category_ids)) {

          val ids: Array[String] = data.pay_category_ids.split(",")
          for (id <- ids) {
            categoryCountAccumulatorV.add(id + "-pay")
          }
        }
      }
    }


    // TODO 4.3 将累加器的数据转化为单一的数据对象
    val accData: mutable.HashMap[String, Long] = categoryCountAccumulatorV.value
    //println(accData.size)

    //(品类id-指标,sum) =》(品类id，[(order,sum),(click,sum),(pay,sum)])
    val groupDate = accData.groupBy {
      case (key, sum) => {
        val keys = key.split("-")
        keys(0)
      }
    }


    val taskId: String = UUID.randomUUID().toString
    val dataCategory: immutable.Iterable[CategoryTop10V2] = groupDate.map {
      case (id, map) => {
        CategoryTop10V2(taskId, id, map.getOrElse(id + "-click", 0L), map.getOrElse(id + "-order", 0L), map.getOrElse(id + "-pay", 0L))
      }
    }

    // TODO 4.4 对转换后的数据进行排序（点击，下单，支付）
    val dataSort = dataCategory.toList.sortWith {
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

    // TODO 4.5 获取前10名的数据
    val top10: List[CategoryTop10V2] = dataSort.take(10)

    //top10.foreach(println)


    // *************************** 需求二 start *********************************************
    /**
      * Top10 热门品类中 Top10 活跃 Session 统计
      */
    // TODO 4.2 将用户访问数据进行筛选过滤（ 点击，前10的品类 ）

    //获取前十的品类id
    val categoryList: List[String] = top10.map(_.categoryId)

    val filterRDD: RDD[UserVisitAction] = dataRDD.filter {
      action => {
        //筛选出点击的数据
        if (action.click_category_id != -1) {
          categoryList.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    }

    // TODO 4.3 将筛选过滤的数据进行结构的转换（categoryid, sessionid, click）( categoryid-sessionid,1 )
    val categoryAndsessionToOneRDD: RDD[(String, Int)] = filterRDD.map {
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    }

    // TODO 4.4 将转换结构后的数据进行聚合( categoryid-sessionid,1 ) ( categoryid-sessionid,sum)
    val categoryAndsessionToSumRDD: RDD[(String, Int)] = categoryAndsessionToOneRDD.reduceByKey(_ + _)

    // TODO 4.5 将聚合后的数据进行结构转换( categoryid-sessionid,sum) ( categoryid, (sessionid,sum))
    val categoryTosessionAndSumRDD: RDD[(String, (String, Int))] = categoryAndsessionToSumRDD.map {
      case (key, sum) => {
        val keys = key.split("_")
        (keys(0), (keys(1), sum))
      }
    }

    // TODO 4.6 将转换结构后的数据按照key进行分组（ categoryid，Iterator[  (sessionid,sum) ]  ）
    val categoryToIteratorRDD: RDD[(String, Iterable[(String, Int)])] = categoryTosessionAndSumRDD.groupByKey()

    // TODO 4.7 将分组后的数据进行排序（降序）
    val resultRDD: RDD[(String, List[(String, Int)])] = categoryToIteratorRDD.mapValues {
      iter => {
        iter.toList.sortBy(_._2).reverse.take(10)
      }
    }
    // TODO 4.8 将排序后的数据获取前10条
    //resultRDD.foreach(println)

    // *************************** 需求二 end *********************************************

    val dataList: RDD[List[CategoryTop10Session10]] = resultRDD.map {
      case (categoryId, list) => {
        list.map {
          case (sessionId, sum) => {
            CategoryTop10Session10(taskId, categoryId, sessionId, sum)
          }
        }
      }
    }

    val categoryDataList: RDD[CategoryTop10Session10] = dataList.flatMap(t => t)

    categoryDataList.foreachPartition {
      datas => {

        // TODO 4.6 将统计结果保存到Mysql中
        val driverClass = ConfigurationUtil.readFile("jdbc.driver.class")
        val url = ConfigurationUtil.readFile("jdbc.url")
        val user = ConfigurationUtil.readFile("jdbc.user")
        val passwd = ConfigurationUtil.readFile("jdbc.password")

        //注册驱动
        Class.forName(driverClass)

        val connection: Connection = DriverManager.getConnection(url, user, passwd)

        val insertSQL = "insert into category values ( ?, ?, ?,? )"

        val preparedStatement = connection.prepareStatement(insertSQL)

        datas.foreach {
          data => {
            preparedStatement.setString(1, data.taskId)
            preparedStatement.setString(2, data.categoryId)
            preparedStatement.setString(3, data.sessionId)
            preparedStatement.setInt(4, data.clickCount)
            preparedStatement.executeUpdate()
          }
        }

        //释放资源
        preparedStatement.close()
        connection.close()

      }
    }

    // 释放资源
    spark.stop()

  }
}

case class CategoryTop10Session10(taskId: String, categoryId: String, sessionId: String, clickCount: Int)