package com.mxl.sparkmall.common.util

import java.util.concurrent.TimeUnit

import okhttp3._

object Okhttp3Client {
  val contentType: MediaType = MediaType.parse("application/json; charset=utf-8")

  val client = new OkHttpClient.Builder()
    .connectTimeout(10, TimeUnit.SECONDS)
    .writeTimeout(10, TimeUnit.SECONDS)
    .readTimeout(30, TimeUnit.SECONDS)
    .build

  //  def get(url: String): String = {
  //    val response = client.newCall(new Request().newBuilder().url(url).build()).execute()
  //    response.body().string()
  //  }

  def post(url: String, json: String): String = {
    RequestBody.create(contentType, json)
    val request: Request = new Request.Builder()
      .url(url)
      .post(RequestBody.create(contentType, json))
      .build

    val response = client.newCall(request).execute
    response.body.string
  }

}
