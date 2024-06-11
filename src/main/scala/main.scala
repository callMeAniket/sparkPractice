import org.apache.spark.sql.SparkSession

object SparkPractice {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Double Numbers")
      .master("local[*]")
      .getOrCreate()

    val numbersRDD = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val doubledRDD = numbersRDD.map(_ * 2)

    doubledRDD.collect().foreach(println)


    val rdd1 = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val doubledRdd = rdd1.map(_ * 2)
    doubledRdd.collect().foreach(println) // Output: 2, 4, 6, 8, 10

    val rdd2 = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val isEven: PartialFunction[Int, Int] = {
      case x if x % 2 == 0 => x
    }

    val evenRdd = rdd2.collect { case x if isEven.isDefinedAt(x) => isEven(x) }
    evenRdd.collect().foreach(println) // Output: 2, 4

    //  implicit def stringToInt(s: String): Int = s.toInt

    val rdd3 = spark.sparkContext.parallelize(Seq("1", "2", "3"))
    val sum = rdd3.map(_ + 1).reduce(_ + _)
    println(sum) // Output: 9
    spark.stop()
  }
}