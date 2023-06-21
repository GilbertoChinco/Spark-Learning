import org.apache.spark.sql.SparkSession
/*
  If you want to work with Spark, you need a driver program. By default shell will become the driver program.
  In core Spark, There are three building blocks for your program

  1. Spark Context: Object used to access spark libraries
  2. RDD
  3. Transformation/actions

  Deployment modes

  local mode: This is the default mode. Here, Spark will run locally on the machine. It will ask the OS to get
  the resources. Entire code will get executed within a single JVM (by default). This mode will work only on
  a single machine

  YARN mode: Here Spark will use YARN to allocate resources and run the code. This means, it can run the
  program in a cluster, i.e. hundreds and thousand of machines.

 More partitions you have, more faster program run
 */

object Project_1 extends App {
  val sparkSession = SparkSession.builder()
    .appName("Project 1")
    .master("local")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  val fileRDD = sc.textFile("Datasets/EL PRINCIPITO.txt", 2)
  val line_words = fileRDD.flatMap( line => line.split(" ") )
  println(line_words.first().mkString(", "))

  val word_count = line_words.map( word => (word, 1) )
  word_count.collect().take(5).foreach(println)

  val words_counts = word_count.reduceByKey(_ + _)
  words_counts.collect().take(5).foreach(println)

  val counts_words = words_counts.map( word_count => (word_count._2, word_count._1) ).sortByKey(false)

  println("Different words used in the book:", counts_words.count())
  val all_words = counts_words.reduce( (x, y) => (x._1 + y._1, x._2) )
  println("Total of numbers of words used in the book:", all_words._1)

  counts_words.collect().take(10).foreach(println)




  //fileRDD.saveAsTextFile("ABCD")
}
