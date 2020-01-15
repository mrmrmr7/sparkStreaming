import org.apache.spark.sql.SparkSession

object StartIt {
  def main(arhs: Array[String]): Unit = {
    print("Я родился")
    val spark = SparkSession
      .builder()
      .appName("StreamingBase")
      .master("local[*]")
      .getOrCreate()

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()

    import spark.implicits._

    val linesFlatMap = lines.as[String].flatMap(_.split(" "))
    val linesGrouped = linesFlatMap.groupBy("value").count()

    val writeToConsole = linesGrouped
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    writeToConsole.awaitTermination()
  }
}