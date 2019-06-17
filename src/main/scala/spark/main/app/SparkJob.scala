

package spark.main.app

import org.apache.spark.sql.SparkSession

object SparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("LogisticRegressionExample")
      .getOrCreate()
    println("Hello from Spark!");
    import spark.implicits._
    val data = spark.read
      .option("header", "true")
      .textFile("./src/main/resources/mushrooms.csv")
      .map(x => x.split(","))
      .filter(x => !x.contains("?"))
      .map(x => Schema(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22)))
      //.filter(x => !x.contains("?"))
      .toDF()
      
    data.printSchema()
    println(data.count())
    println(data.show())
    spark.close()
  }
}