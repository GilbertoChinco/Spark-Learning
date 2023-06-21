package RDDActionTransformation
import org.apache.spark.sql.SparkSession

object BasicsTransformations extends App{

  val sparkSession = SparkSession.builder()
    .appName("Basics Transformation and actions")
    .master("local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val rdd = sc.textFile("Datasets/test.txt")
  println("Creation of the RDD")
  println(rdd.collect()(0))

  //Transformations

  val rdd2 = rdd.flatMap( row => row.split(" ") )
  println("First Transformation (FlatMap), new RDD 2")
  rdd2.take(5).foreach(println)

  val rdd3 = rdd2.map( word => (word, 1) )
  println("Second Transformation (Map), new RDD 3")
  rdd3.take(5).foreach(println)

  val rdd4 = rdd3.filter( word_count => word_count._1.startsWith("a") )
  println("Third Transformation (Filter), new RDD4")
  rdd4.take(5).foreach(println)

  val rdd5 = rdd3.reduceByKey( (x, y) => x + y)
  println("Fourth Transformation (reduceByKey), new RDD5")
  rdd5.take(5).foreach(println)

  val rdd6 = rdd5.map( word_count => (word_count._2, word_count._1) ).sortByKey()
  println("Fourth Transformation (sortByKey), Final Result")
  rdd6.take(5).foreach(println)

  //Actions

  println("Count: ", rdd6.count())

  val firstRec = rdd6.first()
  println(s"First Record: ${firstRec._1}, ${firstRec._2}")

  val datMax = rdd6.max()
  println(s"Max record: ${datMax._1}, ${datMax._2}")

  val totalWordCount = rdd6.reduce( (a, b) => (a._1 + b._1, a._2) )
  println(s"dataReduce Record: ${totalWordCount._1}, ${totalWordCount._2}")




}
