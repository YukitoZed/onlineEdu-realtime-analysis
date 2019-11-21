package com.random.util

import com.alibaba.fastjson.{JSON, JSONObject}

object ParseJsonData {

  def parseObject(data: String): JSONObject = {

    try {
      return JSON.parseObject(data)
    } catch {
      case e: Exception => null
    }
  }
}
