package cn.datalake.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit =  {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
   val textFile = sc.textFile("src/main/resources/shakespeare.txt")
   //val textFile = sc.textFile("hdfs://cndatalake/user/svchadoop/spark/shakespeare.txt")
    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    counts.saveAsTextFile("c:/tmp/shakespeareWordCount"+System.currentTimeMillis())
//    counts.saveAsTextFile("hdfs://cndatalake/user/svchadoop/spark/shakespeareWordCount"+System.currentTimeMillis())
  }

}
