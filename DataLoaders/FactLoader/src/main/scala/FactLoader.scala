import org.apache.spark.sql.functions._

object FactLoader extends App {

  override def main(args: Array[String]) {
    val usage = """
    Usage: factloader data-dir-path
  """
    if (args.length != 1) println(usage)
    else {
      val dir = args(0)
      loadFacts(dir)
    }
  }

  def loadFacts(from: String): Unit = {
    def getInputPathFor(fileName: String) = from + "/" + fileName

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("FactLoader")
      .getOrCreate;

    val calendars = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(getInputPathFor("*Calendar.csv"))

    val listings = spark
      .read
      .format("csv")
      .option("multiline", "true")
      .option("header", "true")
      .option("sep", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(getInputPathFor("*Listings.csv"))

    val listingsSelection = listings
      .select(
        "id",
        "room_type",
        "neighbourhood_cleansed",
        "amenities",
        "host_is_superhost",
        "price"
      )
      .withColumnRenamed("price", "listingPrice")

    val joined = listingsSelection
      .join(calendars, listingsSelection("id") === calendars("listing_id"))

    val joinedSelection = joined
      .select(
        joined("id"),
        joined("date").alias("Time_id"),
        joined("room_type").alias("Room_type_id"),
        joined("neighbourhood_cleansed").alias("Borough_id"),
        joined("host_is_superhost").alias("is_superhost"),
        when(joined("amenities").contains("Internet"), "t").otherwise("f").alias("is_internet"),
        when(joined("amenities").contains("TV"), "t").otherwise("f").alias("is_TV"),
        when(joined("available") === "f", 1).otherwise(0).alias("is_taken"),
        when(joined("available") === "f", regexp_replace(joined("listingPrice"), "[$]", ""))
          .otherwise(0.0).alias("profit"))
      .filter(!_.anyNull)

    val finalDF = joinedSelection
      .groupBy(
        "Borough_id",
        "Time_id",
        "Room_type_id",
        "is_TV",
        "is_internet",
        "is_superhost")
      .agg(
        count("id").alias("rooms_total"),
        sum("is_taken").alias("rooms_rented"),
        sum("profit").alias("rooms_profit"))
      .orderBy("Borough_id", "Time_id", "Room_type_id", "is_TV", "is_internet", "is_superhost")

    finalDF.show(1000)
  }
}