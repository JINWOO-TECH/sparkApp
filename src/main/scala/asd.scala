import org.apache.spark.sql.SparkSession

object TestApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kev12\\OneDrive\\바탕 화면\\study\\hadoop")

    val spark = SparkSession.builder.master("local").appName("my_app").getOrCreate()

//    val rdd = spark.read.textFile("C:\\Users\\User\\Downloads\\README.txt")

    //rdd.write.format("parquet").save("C:\\Users\\User\\Downloads\\test_folder")

    println("Job done")
    spark.stop()

    System.exit(0)

  }
}