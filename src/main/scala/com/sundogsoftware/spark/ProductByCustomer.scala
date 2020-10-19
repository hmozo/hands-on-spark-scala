package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ProductByCustomer {

  def extractCustomerPrice(line: String): (Int, Float) ={
    val fields= line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]", "WordCount")
    val input= sc.textFile("data/SparkScala3/customer-orders.csv")

    val mappedInput= input.map(extractCustomerPrice)
    val totalByCustomer= mappedInput.reduceByKey((x,y) => x+y)
    val totalByAmount= totalByCustomer.map(x => (x._2, x._1))
    val results= totalByCustomer.sortByKey().collect()
    results.foreach(println)

    val results2= totalByAmount.sortByKey().collect()
    results2.foreach(println)




  }
}
