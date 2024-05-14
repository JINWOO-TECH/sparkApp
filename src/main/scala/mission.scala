import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SparkSession, DataFrame,SaveMode}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.commons.io.FileUtils
import java.io.File

object mission {

  def get_period(start_year: Int, end_year: Int, start_month: Int, end_month: Int): collection.mutable.Map[Any, List[Int]] = {
    val return_value = collection.mutable.Map[Any,List[Int]]()  // 변경 가능한 Map으로 선언

    if (start_year > end_year || start_month < 0 || start_month > 12 || end_month < 0 || end_month > 12) {
      return_value += ("flag" -> List())
    }
    else {
      if (start_year == end_year) {
        if (start_month > end_month) {
          return_value += ("flag" -> List())
        }
        else {
          val month_list = (start_month to end_month).toList
          return_value += ("flag" -> List(1), start_year -> month_list)
        }
      }
      else {
        for (year <- start_year to end_year) {
          val month_list =
            if (year == start_year)
              (start_month to 12).toList
            else if (year == end_year)
              (1 to end_month).toList
            else
              (1 to 12).toList
          return_value += ("flag" -> List(1), year -> month_list)
        }
      }
    }
    return_value
  }

  def get_kst(event_date:String): java.sql.Timestamp = {
    val date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
    date_format.setTimeZone(TimeZone.getTimeZone("UTC"))
    // date format에 해당하지 않을 경우 NULL처리
    try {
      val date = date_format.parse(event_date)
      // KST 시간대로 변경
      date_format.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
      val event_date_kst = date_format.format(date) // 문자열로
      val kstParsedDate = date_format.parse(event_date_kst) // 시간으로
      new java.sql.Timestamp(kstParsedDate.getTime)
    }
    catch {
      case e: Exception =>
        null
    }

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kev12\\OneDrive\\바탕 화면\\study\\hadoop")
    val spark = SparkSession.builder.master("local").appName("my_app").getOrCreate()
    val get_kst_udf = udf(get_kst(_: String): java.sql.Timestamp)

    val month_map = Map(1 -> "Jan", 2 -> "Feb", 3 -> "Feb", 4 -> "Mar", 5 -> "Apr", 6 -> "Feb", 7 -> "Jul"
      , 8 -> "Aug", 9 -> "Sep", 10 -> "Oct", 11 -> "Nov", 12 -> "Dec")

    // S 다른 기간 요구 시 작성 S //
    // start_year : 배치 원하는 시작연도, end_year : 배치 원하는 종료연도,
    // start_month : 배치 원하는 시작월, end_month : 배치 원하는 종료월
    val start_year = 2019
    val end_year = 2019
    val start_month = 10
    val end_month = 10
    // E 다른 기간 요구 시 작성 E //

    // csv 파일 데이터 저장 위치
    val data_path = "C:\\Users\\kev12\\OneDrive\\바탕 화면\\study\\data\\"

    // 전처리 후 저장 위치
    var save_path = "C:\\Users\\kev12\\OneDrive\\바탕 화면\\study\\after_etl\\"

    // 요구 시간에 따른 csv 파일 이름
    val map_status = get_period(start_year, end_year, start_month, end_month)
    val flag = map_status("flag")
    map_status -= "flag"
    if (flag.isEmpty){
      println("입력한 값 확인")
    }
    else {
      // S 원하는 csv 파일명 list 반환 S //
      var csv_file_list = ListBuffer[String]()
      map_status.foreach { case (year, month_list) =>
        for (month <- month_list) {
          csv_file_list += data_path + year + "-" + month_map(month) + ".csv"
          // S 지정 년,월 폴더 삭제 S //
          val del_path = save_path + "year=" + year + "\\month=" + month
          println(del_path)
          val directory_path = new File(del_path)
          if (directory_path.exists()) {
            FileUtils.deleteDirectory(directory_path)
          } else {
            println("삭제할 디렉토리가 존재하지 않습니다.")
          }
          // E 지정 년,월 폴더 삭제 E //
        }
      }

      println("--S 대상 list S--")
      println(csv_file_list)
      println("--E 대상 list E--")
      // E 원하는 csv 파일명 list 반환 E //

      //S 대상 파일 dataframe 읽기 S//
      var combined_df: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").csv(csv_file_list.head).limit(0)

      for (csv_file <- csv_file_list) {
        val df = spark.read.option("header", "true").csv(csv_file)
        combined_df = combined_df.union(df)
      }
      //E 대상 파일 dataframe 읽기 E//

      // S utc to kst 변한 S //
      combined_df = combined_df.withColumn("event_time_kst", get_kst_udf(col("event_time")))
      // E utc to kst 변한 E //

      // S daily 파티셔닝을 위한 컬럼 생성 S //
      combined_df = combined_df.withColumn("year", year(col("event_time_kst")))
        .withColumn("month", month(col("event_time_kst")))
        .withColumn("day", dayofmonth(col("event_time_kst")))
      // E daily 파티셔닝을 위한 컬럼 생성 E //

      // S type 변경 S //
      combined_df = combined_df.withColumn("product_id", col("product_id").cast(IntegerType))
        .withColumn("category_id", col("category_id").cast(LongType))
        .withColumn("price", col("price").cast(DoubleType))
        .withColumn("user_id", col("user_id").cast(IntegerType))
      // E type 변경 E //

      // S snappy 및 parquet 포맷으로 저장 (파티셔닝 포함) S //
//      combined_df.write
//        .partitionBy("year", "month", "day") // 년, 월, 일로 파티셔닝
//        .mode(SaveMode.Overwrite) // 저장 모드 설정 (덮어쓰기)
//        .format("parquet") // 저장 포맷 (parquet)
//        .option("compression", "snappy") // 압축 (snappy)
//        .save(save_path)
      // E snappy 및 parquet 포맷으로 저장 (파티셔닝 포함) E //
      println("--- END ---")

    }
    spark.stop()

    System.exit(0)

  }
}
