package com.mxl.sparkmall.api.controller

import com.mxl.sparkmall.api.service.DAUServiceImpl
import io.searchbox.core.Search
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestParam, RestController}

@RestController
class LoggerController {
  @Autowired
  private var dauService: DAUServiceImpl = _

  @GetMapping(Array("/hello"))
  def test01() = {
    "Hello"
  }

  @GetMapping(Array("realtime/dau-count"))
  def dauCount(@RequestParam date:String): Long = {
    dauService.dauCount(date)
  }

  @GetMapping(Array("realtime/dau-count-hour"))
  def dauCountPerHour(): Long = {
    //new Search.Builder()

    0L
  }

}
