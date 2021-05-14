package meteoch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate

case class CHStation(
                      ws: String,
                      wslbl: String,
                      wigosid: String,
                      from: LocalDate,
                      height: Int,
                      coordE: Int,
                      coordN: Int,
                      lat: Double,
                      lon: Double,
                      climReg: String,
                      canton: String)

object Station extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  // Start spark
  val spark = SparkSession.builder().
    master("local[*]").
    appName("MeteoCH Station").
    getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // Load JSON configuration from resources directory
  val cfgJSON = "src/main/resources/cfgXenos_meteoch.json"
  val cfg = spark.read.option("multiLine", true).
    json(cfgJSON).toDF().filter(trim('object) === "Station")
  val inPath = cfg.select('inPath).as[String].collect()(0)
  val chStationFile = cfg.select('chStationFile).as[String].collect()(0)
  val outPath = cfg.select('outPath).as[String].collect()(0)
  val pqFile = cfg.select('pqFile).as[String].collect()(0)

  // Create the schema
  val stationSchema = StructType(Array(
    StructField("Station", StringType),
    StructField("station/location", StringType),
    StructField("WIGOS-ID", StringType),
    StructField("Data since", DateType),
    StructField("Station height m. a. sea level", IntegerType),
    StructField("CoordinatesE", IntegerType),
    StructField("CoordinatesN", IntegerType),
    StructField("Latitude", DoubleType),
    StructField("Longitude", DoubleType),
    StructField("Climate region", StringType),
    StructField("Canton", StringType),
    StructField("URL Previous years (verified data)", StringType),
    StructField("URL Current year", StringType)
  ))

  // Read data using schema and case class
  val df = spark.read.schema(stationSchema).
    option("header", "true").
    option("dateFormat", "dd.MM.yyyy").
    option("sep", ";").
    csv(inPath + chStationFile).
    drop("URL Previous years (verified data)").
    drop("URL Current year").
    withColumnRenamed("Station", "ws").
    withColumnRenamed("station/location", "wslbl").
    withColumnRenamed("WIGOS-ID", "wigosid").
    withColumnRenamed("Data since", "from").
    withColumnRenamed("Station height m. a. sea level", "height").
    withColumnRenamed("CoordinatesE", "coordE").
    withColumnRenamed("CoordinatesN", "coordN").
    withColumnRenamed("Latitude", "lat").
    withColumnRenamed("Longitude", "lon").
    withColumnRenamed("Climate region", "climReg").
    withColumnRenamed("Canton", "canton").
    filter('wigosid.isNotNull).as[CHStation].
    sort('ws).
    cache()

  // Write a parquet file
  df.write.mode(SaveMode.Overwrite).parquet(outPath + pqFile)
  df.show(df.count.toInt, false)
  println(f"${df.count}%,d records written to ${outPath + pqFile}")

  spark.catalog.clearCache()
  spark.stop()
}
