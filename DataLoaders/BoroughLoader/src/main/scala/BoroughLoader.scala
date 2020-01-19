import org.apache.spark.sql.functions._

object BoroughLoader extends App {
  override def main(args: Array[String]) {
    val usage = """
    BoroughLoader needs 1 argument: input_files_dir
  """
    if (args.length != 1) println(usage)
    else {
      val inputDirPath = args(0)
      loadBoroughs(inputDirPath)
    }
  }

  def loadBoroughs(inputPath: String): Unit = {
    def getListingsFileNames: Array[String] = {
      new java.io.File(inputPath)
        .listFiles
        .map(_.getName)
        .filter(_.endsWith("Listings.csv"))
    }

    def getCityFromFileName(fileName: String): String =
      fileName.dropRight(12)

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("BoroughLoader")
      .enableHiveSupport()
      .getOrCreate

    val boroughDF = getListingsFileNames.map(fileName => {
      val path = inputPath + "/" + fileName
      val city = getCityFromFileName(fileName)
      spark
        .read
        .format("csv")
        .option("multiline", "true")
        .option("header", "true")
        .option("sep", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .load(path)
        .select("neighbourhood_cleansed")
        .filter(!_.anyNull)
        .distinct()
        .withColumn("city", lit(city))
    })
      .reduce(_ union _)
      .withColumn("id", monotonically_increasing_id)
      .select("id", "neighbourhood_cleansed", "city")
      .withColumnRenamed("neighbourhood_cleansed", "name")
      .orderBy("id")

    boroughDF.write.mode("append").format("hive").saveAsTable("etl_airbnb.d_borough")
  }
}