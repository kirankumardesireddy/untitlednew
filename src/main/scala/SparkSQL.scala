import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.types._


object SparkSQL extends App {

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


 // val df = spark.read.textFile("/Users/kirand/Documents/Movies.txt")

 val input = sc.textFile("/Users/kirand/Documents/Movies.txt")

//  val input = sc.textFile("s3://kirantestnew.s3.amazonaws.com/Movies.txt")



val df = spark.read.json("/Users/kirand/Documents/people.json")

  case class MovieLine(Line: String)

 val movieline = input.map(line => MovieLine(line))

 movieline.toDF().registerTempTable("MovieLine")

 // df.createOrReplaceTempView("MovieLine")

  //val test = spark.sql("select *  from MovieLine")
  // MovieLine.show()


    val test = spark.sql("select *  from MovieLine")
    test.show()




// val df = spark.read.format("txt").option("header","true").load("/Users/kirand/Documents/Movies.txt")
 //df.show

  // Lets map the date and the genre

  case class DateAndGenre(myDate: String, Genre: String)

  val dateandgenre = input.map(line => line.split(";")).map(s => DateAndGenre( s(0),s(3) ))

  dateandgenre.toDF().registerTempTable("DateAndGenre")


  val test3 = spark.sql("select *  from DateAndGenre")
  test3.show()

  // count how many movies per year

  case class MovieDate(Line: String, myCount: Int)

  val countdate = input.map(line => line.split(";")).map(s => (s(0),1))
  countdate.toDF().createOrReplaceTempView("countdate")


  val test5 = spark.sql("select *  from countdate")
  test5.show()


  val reduceddate = countdate.reduceByKey((a,b) => a + b).map(s => MovieDate(s._1,s._2))

  reduceddate.toDF().createOrReplaceTempView("MovieDate")


  val test6 = spark.sql("select *  from MovieDate")
  test6.show()

  //flatten every word into a new line in the RDD
  val flatmappedinput = input.flatMap(line => line.split(";") )
  flatmappedinput.toDF().createOrReplaceTempView("flatinput")

  val test8 = spark.sql("select *  from flatinput")
  test8.show()


  val inputasdf = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("/Users/kirand/Documents/Movies.txt")
  inputasdf.createOrReplaceTempView("inputdf")



  //val test9 = spark.sql("select Year, Subject from inputdf")
 // test9.show()



  //val test2 = spark.sql("select Year , count(*) from inputdf group by Year ")
  //test2.show()



  //Use this to store the dataframe as parquet on the local drive

//val reduceddf = reduceddate.toDF()
//reduceddf.write.parquet("/Users/kirand/Documents/movie3.parquet")




   //reduceddf.write.option("schema",write_schema).parquet("/Users/kirand/Documents/Movies1.parquet")


  import spark.implicits._
  // Print the schema in a tree format
  //df.printSchema()

 // df.createOrReplaceTempView("ballot")

 // val sqlDF = spark.sql("SELECT * FROM ballot")

//  val test = spark.sql("select PROJECT_NUMBER, PROJECT_TITLE  from ballot")
//  test.show()


  //val result = spark.sql("select PROJECT_TITLE, sum(NIH_FUNDS) from ballot group by PROJECT_TITLE")
  // result.show()

}