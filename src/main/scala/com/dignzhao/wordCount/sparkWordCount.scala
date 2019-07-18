package com.dignzhao.wordCount

import org.apache.spark.{SparkConf, SparkContext}

object sparkWordCount {


  def main(args: Array[String]): Unit = {

    //创建sparkContext对象
    val sparkConf = new SparkConf().setAppName("SparkLocal")
    val sparkContext = new SparkContext(sparkConf)

    //设置打印日志
    sparkContext.setLogLevel("WARN")
    //统计单词个数
    val wordCount = sparkContext.textFile(args(0)).flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)

    wordCount.saveAsTextFile(args(1))
  }

}
