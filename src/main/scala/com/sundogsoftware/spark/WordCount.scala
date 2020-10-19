package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]", "WordCount")

    val input= sc.textFile("data/SparkScala3/book.txt")

    val words= input.flatMap(x => x.split("\\W+"))
    val lowercaseWords= words.map(x => x.toLowerCase())

    val wordCounts= lowercaseWords.countByValue()

    //wordCounts.foreach(println)

    val maxWords= wordCounts.maxBy(_._2)
    println(s"Max: $maxWords")

    //sorting
    val wordCounts2= lowercaseWords.map(x => (x, 1))
      .reduceByKey((x,y) => x + y)

    val a= wordCounts2.map(x => (x._2, x._1))
    val wordCountsSorted= a.sortByKey()

    for(result2 <- wordCountsSorted){
      val count= result2._1
      val word= result2._2
      println(s"$word: $count")
    }



  }

}
