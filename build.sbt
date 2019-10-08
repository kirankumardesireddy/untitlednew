name := "untitled"
version := "0.1"
scalaVersion := "2.12.8"
libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
  "org.apache.spark" %% "spark-core" % "2.4.0",

  "org.apache.spark" %% "spark-sql" % "2.4.0",

  "org.apache.spark" %% "spark-hive" % "2.4.0",
  
  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
    "org.apache.spark" %% "spark-streaming" % "2.4.0",
  
  
  
  
  // https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-twitter
   //   "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"


      // https://mvnrepository.com/artifact/org.apache.kafka/kafka
       "org.apache.kafka" %% "kafka" % "2.3.0",





  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3",

  
  "org.apache.spark" %% "spark-avro" % "2.4.0"


)