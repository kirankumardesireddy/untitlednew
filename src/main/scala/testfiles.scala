import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object testfiles extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("First Application")

  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

 // val testfile = sc.textFile("/Users/kirand/Documents/asfagAnusha.txt")


  //val testfile = sc.textFile("/Users/kirand/Documents/asfagAnusha.txt")


 //  val row = testfile.map(line =>  line.split(","))


 // val linesWithSpark = testfile.filter(line => line.contains("file"))


  //linesWithSpark.count()


  //row.collect()

  //val test = sc.parallelize(List(1, 2, 3, 4)).flatMap(x=> List(x,x)).collect




//  println(linesWithSpark.first())


  val rdd = sc.parallelize(List(
    ("A", 3), ("A", 9), ("A", 12), ("A", 0), ("A", 5),
    ("B", 4), ("B", 10), ("B", 11), ("B", 20), ("B", 25),
    ("C", 32), ("C", 91), ("C", 122), ("C", 3), ("C", 55)), 2)

  val rdd2 = rdd.combineByKey(
    (v: Int) => v.toLong,
    (c: Long, v: Int) => c + v,
    (c1: Long, c2: Long) => c1 + c2)

  rdd2.collect



  //val input = sc.parallelize(List(1, 2, 3, 4))

 //val MapRDD = input.map(x=> x *x)

 // val FilterRDD = input.filter(x => x!=1)

 // println(MapRDD.first())

  //println(MapRDD.take(3))


 // MapRDD.collect()

 // FilterRDD.collect()







  //val result = spark.sql("select PROJECT_TITLE, sum(NIH_FUNDS) from ballot group by PROJECT_TITLE")
  // result.show()


}
