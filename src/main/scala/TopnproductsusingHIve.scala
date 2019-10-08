
//import SparkSQL.{sc, sqlContext}
import SparkSQL.sc
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

import org.apache.spark.sql.functions._



object TopnproductsusingHIve extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("First Application")

  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  //hive sqlContext
  // val sqlContext= new SQLContext(sc)

  // spark sql context
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  import sqlContext.implicits._


  val orders = sc.textFile("/Users/kirand/Documents/orders4.txt")

  case class Orders(
                     order_id: Int,
                     order_date: String,
                     order_customer_id: Int,
                     order_status: String)


  val ordersRDD = orders.map(

    rec => {

      val r = rec.split(",")

      Orders(r(0).toInt, r(1), r(2).toInt, r(3))


    }
  )

  val ordersDF = ordersRDD.toDF()


  val outputPath = "/Users/kirand/Documents/orders1_jsonfile"

  //ordersDF.write.json(outputPath)

   sqlContext.read.json(outputPath).show()



}