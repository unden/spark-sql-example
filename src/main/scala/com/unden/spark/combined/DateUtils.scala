package com.unden.spark.combined

import java.text.SimpleDateFormat

object DateUtils {

  private val threadLocal = new ThreadLocal[SimpleDateFormat]

  def getDateFormat = {
    var dateFormat = threadLocal.get()
    if (dateFormat == null) {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      threadLocal.set(dateFormat)
    }
    dateFormat
  }

  def format(dateStr: String) = {
    val date = getDateFormat.parse(dateStr)
    getDateFormat.format(date)
  }

}
