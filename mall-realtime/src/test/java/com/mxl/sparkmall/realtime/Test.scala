package com.mxl.sparkmall.realtime

import java.text.SimpleDateFormat
import java.util.Date

import org.junit
import org.junit.Test

class Test {

  @junit.Test
  def test02: Unit = {
    val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    println(currentDate)
  }

}
