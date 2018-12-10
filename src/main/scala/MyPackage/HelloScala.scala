package MyPackage

import org.apache.spark.{SparkConf, SparkContext}

object HelloScala {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs:///tmp/shakespeare.txt")

    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count())
    counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount")
  }
}
