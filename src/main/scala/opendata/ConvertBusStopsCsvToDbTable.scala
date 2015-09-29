package src.main.scala.opendata

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import slick.driver.SQLiteDriver.api._

import com.vividsolutions._

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.config.ConfigPerGtfsCsvFile
import src.main.scala.utils.conversion.StringToGeom
import src.main.scala.db.{DbGtfs, DbSchemaStop}


class ConvertBusStopsCsvToDbTable(
     srcGtfsBusStopsCsvFName: String,
     gtfsBusStopsConfigOpts: ConfigPerGtfsCsvFile
  ) extends ConvertGtfsCsvToDbTable(
     srcGtfsBusStopsCsvFName,
     gtfsBusStopsConfigOpts
  ) {


  override def transformCsvHeaderLine(cvs_hdr_line: String): Array[String] =
    cvs_hdr_line.split(",").map(_.trim)

  /*
   * constructor this(srcGtfsCsvDirectory)
   *
   * An auxiliary constructor which assumes that the GTFS Bus Stops CSV file,
   * and the destination WEKA Binary serialized instances will be in their
   * default location.
   */

  def this(srcGtfsCsvDirectory: String) =
    this(srcGtfsCsvDirectory + "/" + Config.gtfsBusStopsInputCsv,
         new ConfigPerGtfsCsvFile(Config.parserGtfsCsv.
                                              minRequiredFieldsBusStops)
        )

  /**
    * this method: <code>parseAndValidateCsvLine</code>
    *
    * Check if the CSV data-line from the GTFS CSV has all the fields
    * that we expect and in the format we expect them, and transform it,
    * returning the array of values for those fields: otherwise, ignore
    * the bad input record, returning null in such a case.
    */

  protected def parseAndValidateCsvLine(dataLine: String): Any = {

    // a better parser is needed for CVS lines with values with ","

    val stdLine = new StringBuilder(dataLine)
    if (stdLine.length > 0 && stdLine.last == ',') stdLine += '0'

    var lineValues: Array[String] = null
    try {
      lineValues = stdLine.split(',').map(_.trim)
    } catch {
      case e: java.util.NoSuchElementException => {
          logMsg(WARNING, "Ignoring: wrong CSV stop: " + dataLine)
          return null
        }
    }

    // We expect that the parsing above returned at least:
    //     "maxRequiredColumn" + 1
    // columns, otherwise, if <= "maxRequiredColumn", it doesn't have enough

    if (lineValues.length <= maxRequiredColumn) {
      logMsg(WARNING, "Ignoring: doesn't have right number of fields: " +
                       dataLine)
      return null
    }

    // Validate that the "stop_lat" and "stop_lon" have a valid point

    val colNumberStopLatit = posRequiredHeaderFields("stop_lat")
    val colNumberStopLongit = posRequiredHeaderFields("stop_lon")

    var stopGeom: jts.geom.Geometry = null
    try {
      stopGeom = StringToGeom(
                               lineValues(colNumberStopLongit) + "," +
                               lineValues(colNumberStopLatit) + " "
                             )
    } catch {
      case e: java.text.ParseException => {
          logMsg(WARNING, "Ignoring: wrong time in CSV line: " + dataLine)
          return null
        }
    }

    /*
     * Some public transit agencies, like Vancouver Translink, don't give this
     * field "wheelchair_boarding" in their "stops.txt" GTFS CSV file
     *
    // We validate that the field "wheelchair_boarding" is an integer
    val posWheelchairBoarding = posRequiredHeaderFields("wheelchair_boarding")
    val strWheelchairBoarding = lineValues(posWheelchairBoarding)
    var intWheelchairBoarding: Int = -1000000
    try {
      intWheelchairBoarding = strWheelchairBoarding.toInt
    } catch {
      case e: NumberFormatException => {
                 logMsg(WARNING, "Ignoring: not an integer in " +
                        (posWheelchairBoarding + 1) + " column: value: '" +
                          strWheelchairBoarding + "'; line: " + dataLine)
                 return null
              }
    }

    if (intWheelchairBoarding < 0) {
      logMsg(WARNING, "Ignoring: not a non-negative int in " +
                       (posWheelchairBoarding + 1) + " field: value: " +
                     intWheelchairBoarding + "; line: " +
                       dataLine)
      return null
    }
    *
    *
    */

    // Return the SQL record (Scala case class) with our required fields in
    // this GTFS CSV line (the other fields in this GTFS CSV line that we
    // don't require are ignored and not written to the database)
  
    val fieldsToCols = posRequiredHeaderFields
    val stopId = lineValues(fieldsToCols("stop_id"))
    val stopName = lineValues(fieldsToCols("stop_name"))
    val stopLatitude = lineValues(fieldsToCols("stop_lat")).toDouble
    val stopLongitude = lineValues(fieldsToCols("stop_lon")).toDouble
  
    logMsg(DEBUG, s"Valid stop: $stopId, $stopName")
    return DbSchemaStop(stopId, stopName, stopLatitude, stopLongitude)

  } // method parseAndValidateCsvLine


  override protected def insertRecordsIntoDb(seqRecords: Seq[Any],
                                             dstGtfsDb: DbGtfs) {

    val seqBusStops = seqRecords.asInstanceOf[Iterable[DbSchemaStop]]

    val insert = DBIO.seq(dstGtfsDb.busStops ++= seqBusStops).transactionally

    // dstGtfsDb.db.run(insert)
    Await.ready(dstGtfsDb.db.run(insert), Duration.Inf)
  }

} // class ConvertBusStopsCsvToWekaSerializedInsts

