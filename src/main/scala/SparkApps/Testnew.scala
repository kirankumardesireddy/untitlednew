package SparkApps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Testnew  extends App {

  val mymap:Map[Int,String] = Map(102-> "Min" , 103->"Msd")

  println(mymap)

  println(mymap(103))
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("First Application")

  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val sqlContext= new org.apache.spark.sql.SQLContext(sc)


  //For implicit conversions like converting RDDs to DataFrames
  //import spark.implicits._

  import sqlContext.implicits._


  val df = spark.read
    .format("avro")
    .option("mode","failfast")
    .load("/Users/kirand/Documents/salesavro-data/")


  //Write Data Frame to CSV
  df.write
    .format("csv")
    .option("header","true")
    .option("nullValue","NA")
    .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
    .mode("overwrite")
    .save("/Users/kirand/Documents/sales-csvdata/")

}
