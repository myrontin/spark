package spark_streaming


import org.apache.spark._
import org.apache.spark.streaming._


object word_count {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9981)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    
    ssc.start()
    ssc.awaitTermination()

  }

}