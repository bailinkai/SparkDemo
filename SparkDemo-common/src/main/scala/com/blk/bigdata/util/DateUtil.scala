package com.blk.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  def parseTimestampToString(time: Long,f:String = "yyyy-MM-dd HH:mm:ss"): String ={

    val date = new Date(time)
    val dateFormat = new SimpleDateFormat(f)
    dateFormat.format(date)
  }

  def parseTimestamp(time: String,f:String = "yyyy-MM-dd HH:mm:ss"): String ={
    parseTimestampToString(time.toLong)

  }

  def parseTimestampToLong(time: String,f:String = "yyyy-MM-dd HH:mm:ss"): Long ={
    val dateFormat = new SimpleDateFormat(f)
    val date = dateFormat.parse(time)
    date.getTime
  }

}

