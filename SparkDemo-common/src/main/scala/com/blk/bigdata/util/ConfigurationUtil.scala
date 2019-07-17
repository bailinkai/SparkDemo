package com.blk.bigdata.util

import java.util.ResourceBundle

import com.alibaba.fastjson.JSON

object ConfigurationUtil {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  private val condBundle: ResourceBundle = ResourceBundle.getBundle("condition")

  def readFile(key: String) = {
    val value = bundle.getString(key)
    value
  }

  def getValueByJsonKey(jsonKey:String): String ={
    val jsonStr = condBundle.getString("condition.params.json")
    val jSONObject = JSON.parseObject(jsonStr)
    jSONObject.getString(jsonKey)
  }

  def main(args: Array[String]): Unit = {
    //println(readFile("redis.host"))
    println(getValueByJsonKey("startDate"))
  }

}
