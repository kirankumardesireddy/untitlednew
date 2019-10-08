package SparkApps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Testnew4  extends App {

  val mymap:Map[Int,String] = Map(102-> "Min" , 103->"Msd")

  println(mymap)

  println(mymap(103))

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()


 // spark.sparkContext().textFile("/Users/kirand/Documents/Movies.txt")

  val a = spark.read.text("/Users/kirand/Documents/Movies.txt")

//  a.show()

  val c = a.rdd

  println(c.count())


  //val input = sc.textFile("/Users/kirand/Documents/Movies.txt")













}
