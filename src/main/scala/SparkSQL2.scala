//import SparkSQL.{sc, sqlContext}
import SparkSQL.sc
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

import org.apache.spark.sql.functions._



object SparkSQL2 extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("First Application")

  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()



  // spark sql context
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)




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

  val ordersDF = ordersRDD .toDF()

 // ordersDF.show()

  val order_items = sc.textFile("/Users/kirand/Documents/order_items.txt")

  case class OrderItems(
                         order_item_id: Int,
                         order_item_order_id: Int,
                         order_item_product_id: Int,
                         order_item_quantity: Int,
                         order_item_subtotal: Float,
                         order_item_price: Float)


  val ordersitemsRDD = order_items.map(

    rec => {

      val r = rec.split(",")

      OrderItems(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4).toFloat, r(5).toFloat)


    }
  )

  val ordersitemsDF = ordersitemsRDD .toDF()

 // ordersitemsDF.show()

  val ordersFiltered = ordersDF.filter(ordersDF("order_status") === "COMPLETE")

  val ordersJoin = ordersFiltered.join(ordersitemsDF,
    ordersFiltered("order_id") === ordersitemsDF("order_item_order_id"))

  val outputPath = "/Users/kirand/Documents/orders1_jsonfile"

  sqlContext.setConf("spark.sql.shuffle.partitions", "1")

  //ordersJoin.
  //  groupBy("order_date").
  //  agg(sum("order_item_subtotal")).
  //  sort("order_date").
  //  rdd.saveAsTextFile(outputPath)


  ordersDF.registerTempTable("Orderstable")


  val ordersnew = spark.sql("select order_status, count(1) order_count  from Orderstable   group by order_status")
  //ordersnew.show()

  ordersitemsDF.registerTempTable("Orderitemtable")


  val orderitemstotal = spark.sql("select  Orderstable.order_date, sum(Orderitemtable.order_item_subtotal)  order_item_total  from Orderstable join Orderitemtable on " +
    "Orderstable.order_id = Orderitemtable.order_item_order_id " +
    "where Orderstable.order_status ='COMPLETE' " +
    "group by Orderstable.order_date")
  //orderitemstotal.rdd.saveAsTextFile(outputPath)




  ordersDF.write.json(outputPath)





}
