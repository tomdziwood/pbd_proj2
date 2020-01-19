import org.apache.spark.sql.functions._

object FactLoader extends App {

  override def main(args: Array[String]) {
    val usage = """
    FactLoader needs 1 argument: input_files_dir
  """
    if (args.length != 1) println(usage)
    else {
      val inputDir = args(0)
      loadFacts(inputDir)
    }
  }

  def loadFacts(from: String): Unit = {
    def getInputPathFor(fileName: String) = from + "/" + fileName

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("FactLoader")
      .enableHiveSupport()
      .getOrCreate;

    val calendars = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
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
        joined("date"),
        joined("room_type"),
        joined("neighbourhood_cleansed").alias("borough"),
        when(joined("host_is_superhost").contains("t"), true).otherwise(false).alias("is_superhost"),
        when(joined("amenities").contains("Internet"), true).otherwise(false).alias("is_internet"),
        when(joined("amenities").contains("Kitchen"), true).otherwise(false).alias("is_kitchen"),
        when(joined("available") === "f", 1).otherwise(0).alias("is_taken"),
        when(joined("available") === "f", regexp_replace(joined("listingPrice"), "[$]", ""))
          .otherwise(0.0).alias("profit"))
      .filter(!_.anyNull)

    val rawFacts = joinedSelection
      .groupBy(
        "borough",
        "date",
        "room_type",
        "is_kitchen",
        "is_internet",
        "is_superhost")
      .agg(
        count("id").alias("rooms_total"),
        sum("is_taken").alias("rooms_rented"),
        sum("profit").alias("rooms_profit"))
      .orderBy("borough", "date", "room_type", "is_kitchen", "is_internet", "is_superhost")

    val boroughs = spark.sql("select * from etl_airbnb.d_borough")
    val times = spark.sql("select * from etl_airbnb.d_time")
    val roomTypes = spark.sql("select * from etl_airbnb.d_room_type")

    val factsWithBoroughs = rawFacts
      .join(boroughs, rawFacts("borough") === boroughs("name"))
      .withColumnRenamed("id", "id_borough")
        .select(
          "id_borough",
          "date",
          "room_type",
          "rooms_total",
          "rooms_rented",
          "rooms_profit",
          "is_kitchen",
          "is_internet",
          "is_superhost"
        )

    val factsWithBoroughsExt = factsWithBoroughs
      .withColumn("year", factsWithBoroughs("date").substr(0, 4))
      .withColumn("month", factsWithBoroughs("date").substr(6, 2))
      .withColumn("day", factsWithBoroughs("date").substr(9, 2))

    val factsWithTimes = factsWithBoroughsExt
      .join(times, factsWithBoroughsExt("year") === times("year") &&
                   factsWithBoroughsExt("month") === times("month") &&
                   factsWithBoroughsExt("day") === times("day"))
      .withColumnRenamed("id", "id_time")
      .select(
        "id_borough",
        "id_time",
        "room_type",
        "rooms_total",
        "rooms_rented",
        "rooms_profit",
        "is_kitchen",
        "is_internet",
        "is_superhost"
      )

    val finalFacts = factsWithTimes
      .join(roomTypes, factsWithTimes("room_type") === roomTypes("name"))
      .withColumnRenamed("id", "id_room_type")
      .select(
        "id_borough",
        "id_room_type",
        "id_time",
        "rooms_total",
        "rooms_rented",
        "rooms_profit",
        "is_superhost",
        "is_internet",
        "is_kitchen"
      )

    finalFacts.write.mode("append").format("hive").saveAsTable("etl_airbnb.f_facts")
  }
}