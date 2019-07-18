package com.mxl.sparkmall.common.bean

/**
  * 用户信息实体
  * @param user_id      用户 id
  * @param username     用户的登录名
  * @param name         用户的昵称或真实名
  * @param age          用户年龄
  * @param professional 用户的职业
  * @param gender       用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    gender: String)

object UserInfo{
  def main(args: Array[String]): Unit = {
    print("Hello")
  }
}
