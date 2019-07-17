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

object Req1CategoryTop10Application {
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

    //top10s.foreach(println)

    // TODO 4.6 将统计结果保存到Mysql中
    val driverClass = ConfigurationUtil.readFile("jdbc.driver.class")
    val url = ConfigurationUtil.readFile("jdbc.url")
    val user = ConfigurationUtil.readFile("jdbc.user")
    val passwd = ConfigurationUtil.readFile("jdbc.password")

    //注册驱动
    Class.forName(driverClass)

    val connection: Connection = DriverManager.getConnection(url,user,passwd)

    //val insertSQL = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values ( ?, ?, ?, ?, ? )"
    val insertSQL = "insert into category ( taskId, category_id, click_count, order_count, pay_count ) values ( ?, ?, ?, ?, ? )"

    val preparedStatement = connection.prepareStatement(insertSQL)

    top10.foreach{
      data => {
        preparedStatement.setString(1,data.taskId)
        preparedStatement.setString(2,data.categoryId)
        preparedStatement.setLong(3,data.clickCount)
        preparedStatement.setLong(4,data.orderCount)
        preparedStatement.setLong(5,data.payCount)
        preparedStatement.executeUpdate()
      }
    }

    //释放资源
    preparedStatement.close()
    connection.close()
    // 释放资源
    spark.stop()

  }
}

//样例类
case class CategoryTop10V2(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

//自定义累加器
class CategoryCountAccumulatorV2 extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new CategoryCountAccumulatorV2

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value

    //合并两个map
    map = map1.foldLeft(map2) {
      case (innerMap, (key, sum)) => {
        innerMap(key) = innerMap.getOrElse(key, 0L) + sum
        innerMap
      }
    }

  }

  override def value: mutable.HashMap[String, Long] = map
}