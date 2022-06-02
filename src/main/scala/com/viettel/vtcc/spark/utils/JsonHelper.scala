package com.viettel.vtcc.spark.utils

import org.json4s.native.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, JValue}

object JsonHelper {

  implicit val formats = DefaultFormats

  def write[T <: AnyRef](value: T): String = Serialization.write(value)

  def parse(json: String): JValue = JsonMethods.parse(json)
}
