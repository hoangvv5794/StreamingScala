package com.viettel.vtcc.spark.process

import scala.io.Source

/**
 * Hello world!
 *
 */

object SparkSessionTest extends App {


  var stock_map: Map[String, String] = Map()

  // create map stock id and broad name
  val source = Source.fromFile("C:\\Users\\KGM\\Downloads\\stock_telegram\\industry_stock_id_broad_band.txt")
  for (line <- source.getLines()) {
    val first_value = line.split("\t")(0)
    val second_value = line.split("\t")(1)
    stock_map += (first_value -> second_value)
  }
  println(stock_map)
  source.close()

}
