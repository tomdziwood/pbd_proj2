import org.apache.spark.sql.functions.monotonically_increasing_id

object RoomTypeLoader extends App {
  override def main(args: Array[String]) {
    val usage = """
    RoomTypeLoader needs 1 argument: input_files_dir
  """
    if (args.length != 1) println(usage)
    else {
      val inputDirPath = args(0)
      loadRoomTypes(inputDirPath)
    }
  }

  def loadRoomTypes(from: String): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("RoomTypeLoader")
      .enableHiveSupport()
      .getOrCreate

    val roomTypes = spark
      .read
      .format("csv")
      .option("multiline", "true")
      .option("header", "true")
      .option("sep", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(from + "/*Listings.csv")
      .select("room_type")
      .filter(!_.anyNull)
      .distinct()

    val finalDF = roomTypes
      .withColumn("id", monotonically_increasing_id)
      .withColumnRenamed("room_type", "name")
      .select(
        "id",
        "name"
      )

    finalDF.write.mode("append").format("hive").saveAsTable("etl_airbnb.d_room_type")
  }
}
