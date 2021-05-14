package meteoch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.time.LocalDate

case class CHData(
                   wslbl: String,
                   date: LocalDate,
                   gre000d0: Int,
                   hto000d0: Int,
                   nto000d0: Int,
                   prestad0: Double,
                   rre150d0: Int,
                   sre000d0: Int,
                   tre200d0: Double,
                   tre200dn: Double,
                   tre200dx: Double,
                   ure200d0: Double
                 )

object Data extends App {
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
    .json(cfgJSON).toDF().filter(trim('object) === "Data")
  val inPath = cfg.select('inPath).as[String].collect()(0)
  //  val chStationFile = cfg.select('chStationFile).as[String].collect()(0)
  val outPath = cfg.select('outPath).as[String].collect()(0)
  val pqHFile = cfg.select('pqHFile).as[String].collect()(0)
  val pqRFile = cfg.select('pqRFile).as[String].collect()(0)


  val dataSchema = StructType(Array(
    StructField("station/location", StringType),
    StructField("date", DateType),
    StructField("gre000d0", IntegerType), //W/m² Globalstrahlung; Tagesmittel
    StructField("hto000d0", IntegerType), //cm   Gesamtschneehöhe; Morgenmessung von 6 UTC
    StructField("nto000d0", IntegerType), //%    Gesamtbewölkung; Tagesmittel
    StructField("prestad0", DoubleType), //hPa   Luftdruck auf Stationshöhe (QFE); Tagesmittel
    StructField("rre150d0", IntegerType), //mm   Niederschlag; Tagessumme 6 UTC - 6 UTC Folgetag
    StructField("sre000d0", IntegerType), //min  Sonnenscheindauer; Tagessumme
    StructField("tre200d0", DoubleType), //°C    Lufttemperatur 2 m über Boden; Tagesmittel
    StructField("tre200dn", DoubleType), //°C    Lufttemperatur 2 m über Boden; Tagesminimum
    StructField("tre200dx", DoubleType), //°C    Lufttemperatur 2 m über Boden; Tagesmaximum
    StructField("ure200d0", DoubleType) //%      Relative Luftfeuchtigkeit 2 m über Boden
  ))

  // Read MCHStation.parquet file to loop over wslbl
  val lblList = spark.read.parquet("MCHStation.parquet").
    select('wslbl).groupBy('wslbl).count.select('wslbl).
    as[String].collect().toList

  // Read data using schema and case class
  val period = List("current", "previous")
  var df: Dataset[CHData] = null
  var dfR: Dataset[CHData] = null
  var dfH: Dataset[CHData] = null
  for (p <- period) {
    var firstCanton = true
    for (l <- lblList) {
      val chDataFile = s"nbcn-daily_${l}_${p}.csv"
      if (firstCanton) {
        df = spark.read.schema(dataSchema).
          option("header", "true").
          option("dateFormat", "yyyyMMdd").
          option("sep", ";").
          csv(inPath + chDataFile).
          withColumnRenamed("station/location", "wslbl").
          as[CHData]
        firstCanton = !firstCanton
      } else {
        df = df.union(spark.read.schema(dataSchema).
          option("header", "true").
          option("dateFormat", "yyyyMMdd").
          option("sep", ";").
          csv(inPath + chDataFile).
          withColumnRenamed("station/location", "wslbl").
          as[CHData])
      }
    } // lblList
    if (p == "current") dfR = df.cache() else dfH = df.cache()
  } // period

  // Write parquet files
  dfR.write.mode(SaveMode.Overwrite).parquet(outPath + pqRFile)
  dfR.show(false)
  println(f"${dfR.count}%,d recent records written to ${outPath + pqRFile}")

  dfH.write.mode(SaveMode.Overwrite).parquet(outPath + pqHFile)
  dfH.show(false)
  println(f"${dfH.count}%,d historical records written to ${outPath + pqHFile}")

  spark.catalog.clearCache()
  spark.stop()
}
