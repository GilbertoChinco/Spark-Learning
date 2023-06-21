package RDDActionTransformation
import org.apache.spark.sql.SparkSession

object BasicsActions extends App{
  val sparkSession = SparkSession.builder()
    .appName("Basics actions operations")
    .master("local")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  val inputList = List(("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60))
  val inputRDD = sc.parallelize(inputList)
  val listRDD = sc.parallelize(List(1, 2, 3, 4, 5, 3, 2))

  //aggregate
  def param0(accu: Int, v: Int): Int = accu + v
  def param1(accu1: Int, accu2: Int): Int = accu1 + accu2
  println("aggregate: ", listRDD.aggregate(0)(param0, param1))

  def param3(accu: Int, v: (String, Int)): Int = accu + v._2
  def param4(accu1: Int, accu2: Int): Int = accu1 + accu2
  println("aggregate: ", inputRDD.aggregate(0)(param3, param4))

  //reduce
  println("reduce: ", listRDD.reduce(_ + _))
  println("reduce alternate:", listRDD.reduce((x, y) => x + y))
  println("reduce", inputRDD.reduce((x, y) => ("Total", x._2 + y._2)))

  //collect
  println("collect: ")
  listRDD.collect().foreach(println)

  //count, countApprox, countApproxDistinct
  println("Count", listRDD.count())
  println("CountApprox", listRDD.countApprox(1200))
  println("countApproxDistinct:", listRDD.countApproxDistinct())

  //take
  println("take", listRDD.take(3).mkString(","))

  //first
  println("fisrt:", inputRDD.first())

  //top
  println("top", inputRDD.top(3).mkString(","))

  //min, max
  println("Minimum value: ", inputRDD.min(), "Maximum values:", inputRDD.max())
}