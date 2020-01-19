import org.apache.spark.sql.functions.monotonically_increasing_id

object TimeLoader extends App {
  override def main(args: Array[String]) {
    val usage = """
    TimeLoader needs 1 argument: input_files_dir
  """
    if (args.length != 1) println(usage)
    else {
      val inputDirPath = args(0)
      loadTimes(inputDirPath)
    }
  }

  def loadTimes(from: String): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("TimeLoader")
      .enableHiveSupport()
      .getOrCreate

    val dates = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load(from + "/*Calendar.csv")
      .select("date")
      .filter(!_.anyNull)
      .distinct()
      .withColumn("id", monotonically_increasing_id)

    val finalDF = dates
      .select(
        dates("id"),
        dates("date").substr(0, 4).as("year"),
        dates("date").substr(6, 2).as("month"),
        dates("date").substr(9, 2).as("day")
      )

    finalDF.write.mode("append").format("hive").saveAsTable("etl_airbnb.d_time")
  }
}
