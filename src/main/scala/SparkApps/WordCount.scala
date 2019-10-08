package SparkApps

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {

 //   val inputFile = args(0)
  //  val outputFile = args(1)
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/kirand/Documents/testdata2.csv")
    val words = input.flatMap(_.split(","))
    val counts = words.map(word => (word,1)).reduceByKey(_+_)

  counts.collect()

  }


}
