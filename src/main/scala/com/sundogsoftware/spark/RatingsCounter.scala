package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]", "RatingsCounter")

    val lines= sc.textFile("data/ml-100k/u.data")

    val pairs= lines.map(s => (s,1))
    //pairs.foreach(println)




    val ratings= lines.map(x => x.toString.split("\t")(2))

    val results= ratings.countByValue()

    val sortedResults= results.toSeq.sortBy(_._1)

    sortedResults.foreach(println)

    // RDD
    val rdd= sc.parallelize(List(1, 2, 3, 4))
    val squares= rdd.map(x => x*x)

    squares.foreach(println)

    def parseLine(line:String): (Int, Int) ={
      val fields= line.split(',')
      val age= fields(2).toInt
      val numFriends= fields(3).toInt
      (age, numFriends)
    }

    val lines2= sc.textFile("data/SparkScala3/fakefriends.csv")
    val rdd2= lines2.map(parseLine)

    val friendsByAge= rdd2.mapValues(x=>(x,1))
      .reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))

    val averagesByAge= friendsByAge.mapValues(x=>x._1/x._2)
    val results2= averagesByAge.collect()
    results2.sorted.foreach(println)

    println(s"${averagesByAge.values.max()}")
    println(results2.maxBy(_._2))

    println(results2.minBy(_._2))

  }
}
