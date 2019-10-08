package SparkApps

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object FileSparkStream {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MyfirstStreamingAp").set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(conf, Seconds(30))
    val lines = ssc.textFileStream("/Users/kirand/Documents/testfile")
    lines.flatMap (x => x.split(" ") ).map ( x => (x,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}