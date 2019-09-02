package com.venn.common

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by venn on 19-4-25.
  */
object DataUtils {

  val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def timestampToDate(timestamp:Long) : String = {
    val date = new Date(timestamp)
    format.format(date)
  }

}
