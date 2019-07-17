package com.blk.bigdata.util

object StringUtil {

  def isNotEmpty(s:String): Boolean ={
    s != null && !"".equals(s.trim)
  }
}
