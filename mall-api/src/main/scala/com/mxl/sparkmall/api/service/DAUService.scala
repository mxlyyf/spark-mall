package com.mxl.sparkmall.api.service

trait DAUService {
  //日活统计
  def dauCount(date: String): Long

  //日活各个小时明细统计
  def dauCountPerHour(date: String): Map[String, Long]
}
