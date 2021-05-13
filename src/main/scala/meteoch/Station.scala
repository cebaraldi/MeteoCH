package meteoch

import org.apache.log4j.{Level, Logger}
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
  val cfg = spark.read.option("multiLine", true)
    .json(cfgJSON).toDF().filter('object === "Station")
  val inPath = cfg.select('inPath).as[String].collect()(0)
  val chStationFile = cfg.select('chStationFile).as[String].collect()(0)
  val outPath = cfg.select('outPath).as[String].collect()(0)
  val pqFile = cfg.select('pqFile).as[String].collect()(0)

  // Create the schema
  val stationSchema = StructType(Array(
    StructField("ws", StringType),
    StructField("wslbl", StringType),
    StructField("wigosid", StringType),
    StructField("from", DateType),
    StructField("height", IntegerType),
    StructField("coordE", IntegerType),
    StructField("coordN", IntegerType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("climReg", StringType),
    StructField("canton", StringType)
  ))

  // Read data using schema and case class
  val df = spark.read.schema(stationSchema).
    option("header", "true").
    option("dateFormat", "dd.MM.yyyy").
    option("sep", ";").
    csv(inPath + chStationFile).as[CHStation].
    filter('wigosid.isNotNull).
    sort('ws).
    cache()

  // Write a parquet file
  df.write.mode(SaveMode.Overwrite).parquet(outPath + pqFile)
  df.show(df.count.toInt,false)

  spark.stop()
}
