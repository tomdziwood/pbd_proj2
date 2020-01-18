import org.apache.spark.sql.functions._

object BoroughLoader extends App {
  override def main(args: Array[String]) {
    val usage = """
    Usage: boroughloader input_path output_path
  """
    if (args.length != 2) println(usage)
    else {
      val inputPath = args(0)
      val outputPath = args(1)
      loadBoroughs(inputPath, outputPath)
    }
  }

  def loadBoroughs(inputPath: String, outputPath: String): Unit = {
    def getListingsFileNames: Array[String] = {
      new java.io.File(inputPath)
        .listFiles
        .map(_.getName)
        .filter(_.endsWith("Listings.csv"))
    }

    def getCityFromFileName(fileName: String): String =
      fileName.dropRight(12)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("BoroughLoader")
      .getOrCreate

    val sc = spark.sparkContext

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

    boroughDF.show(100)

  }
}