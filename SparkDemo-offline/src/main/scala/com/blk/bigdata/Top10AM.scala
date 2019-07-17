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

object Top10AM {
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

    //测试查询结果
    //println(top1s)

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

    top1s.foreach{
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
    spark.stop()
  }
}

//样例类
case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    val map1 = map
    val map2 = other.value

    map = map1.foldLeft(map2) {
      case (innerMap, t) => {
        innerMap(t._1) = innerMap.getOrElse(t._1, 0L) + t._2
        innerMap
      }
    }

  }

  override def value: mutable.HashMap[String, Long] = map
}
