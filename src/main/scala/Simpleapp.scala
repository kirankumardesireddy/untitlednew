
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.types._







object Simpleapp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)


    val logFile = "https://bigdatalabsnew.s3.amazonaws.com/testfile.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = sc.textFile(logFile).cache()
    val numHadoop = logData.filter(line => line.contains("Hadoop")).count()
    val numScala = logData.filter(line => line.contains("Scala")).count()
    println(s"Lines with Hadoop: $numHadoop, Lines with Scala: $numScala")
  }
}





