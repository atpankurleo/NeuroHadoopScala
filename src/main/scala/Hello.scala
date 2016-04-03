package main.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Ankur on 3/12/2016.
  */
object Hello {

  def main(args :Array[String]){
    val conf = new SparkConf().setMaster("local[1]").setAppName("HDFS to SparkSQL")
    val sc: SparkContext = new SparkContext(conf)
    //val textData = sc.textFile("data.txt")
    //val txtMap = textData.map{ x => x.split("\\W+")}


    val txtmap = sc.textFile("sample.txt")
      .flatMap(x => x.split("\\W+"))
      .map(x => (x.toLowerCase, 1))
      .reduceByKey(_ + _)

    println(txtmap.first())

    val sortKey = txtmap.sortByKey()
                        .top(10)
    println(sortKey.length)

    println(txtmap)

    val top10RDD = sc.parallelize(sortKey, 1)
    //top10RDD.saveAsTextFile("sparkhello")
    //println(txtmap)
    //txtMap.take(5)
    //println(txtMap.collect().length)
    sc.stop()
    System.exit(0)
  }

}
