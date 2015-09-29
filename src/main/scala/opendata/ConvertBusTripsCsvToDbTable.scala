package src.main.scala.opendata

import _root_.java.util.Date

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import slick.driver.SQLiteDriver.api._

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.config.ConfigPerGtfsCsvFile
import src.main.scala.utils.conversion.ConvertTimeToSeconds
import src.main.scala.db.{DbGtfs, DbSchemaTrip}


class ConvertBusTripsCsvToDbTable(
     srcGtfsBusTripsCsvFName: String,
     gtfsBusTripsConfigOpts: ConfigPerGtfsCsvFile
  ) extends ConvertGtfsCsvToDbTable(
     srcGtfsBusTripsCsvFName,
     gtfsBusTripsConfigOpts
  ) {

  // These are the senses of direction of the trip in the General
  // Transit Feed Specification format

  final val BusTripOutboundDirection = "1"  // going
  final val BusTripInboundDirection = "0"   // returning

  override def transformCsvHeaderLine(cvs_hdr_line: String): Array[String] =
    cvs_hdr_line.split(",").map(_.trim)

  /*
   * constructor this(srcGtfsCsvDirectory)
   *
   * An auxiliary constructor which assumes that the GTFS Bus Trips CSV file,
   * and the destination WEKA Binary serialized instances will be in their
   * default location.
   */

  def this(srcGtfsCsvDirectory: String) =
    this(srcGtfsCsvDirectory + "/" + Config.gtfsTripsInputCsv,
         new ConfigPerGtfsCsvFile(Config.parserGtfsCsv.
                                              minRequiredFieldsBusTrips)
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

    var lineValues: Array[String] = null
    try {
      lineValues = dataLine.split(",").map(_.trim)
    } catch {
      case e: java.util.NoSuchElementException => {
          logMsg(WARNING, "Ignoring: wrong CSV trip: " + dataLine)
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

    // We validate that the fields "direction_id" and "wheelchair_accessible"
    // are non-negative numbers (ie., that are >= 0)
    val posDirectionId = posRequiredHeaderFields("direction_id")
    val strDirectionId = lineValues(posDirectionId)
    var intDirectionId: Int = -1
    try {
      intDirectionId = strDirectionId.toInt
    } catch {
      case e: NumberFormatException => {
                 logMsg(WARNING, "Ignoring: not an integer in " +
                          (posDirectionId + 1) + " column: value: '" +
                          strDirectionId + "'; line: " + dataLine)
                 return null
              }
    }

    if (intDirectionId < 0) {
      logMsg(WARNING, "Ignoring: not a non-negative int in " +
                       (posDirectionId + 1) + " field: value: " +
                       intDirectionId + "; line: " +
                       dataLine)
      return null
    }

    /*
     * Some agencies, like Vancouver Translink, don't give this field
     * of "wheelchair_accessible" (we could assume that it is "1")
     *
     *
    val posWheelchairAcces = posRequiredHeaderFields("wheelchair_accessible")
    val strWheelchairAcces = lineValues(posWheelchairAcces)
    var intWheelchairAcces: Int = -1
    try {
      intWheelchairAcces = strWheelchairAcces.toInt
    } catch {
      case e: NumberFormatException => {
                 logMsg(WARNING, "Ignoring: not an integer in " +
                          (posWheelchairAcces + 1) + " column: value: '" +
                          strWheelchairAcces + "'; line: " + dataLine)
                 return null
              }
    }

    if (intWheelchairAcces < 0) {
      logMsg(WARNING, "Ignoring: not a non-negative int in " +
                       (posWheelchairAcces + 1) + " field: value: " +
                       intWheelchairAcces + "; line: " +
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
    val routeId = lineValues(fieldsToCols("route_id"))
    val serviceId = lineValues(fieldsToCols("service_id"))
    val tripId = lineValues(fieldsToCols("trip_id"))
    val shapeId = lineValues(fieldsToCols("shape_id"))
    val tripHeadSign = lineValues(fieldsToCols("trip_headsign"))

    logMsg(DEBUG, s"Valid trip: $routeId, $tripId: $tripHeadSign")
    return DbSchemaTrip(routeId, serviceId, tripId, intDirectionId, shapeId,
                        tripHeadSign)

  } // method parseAndValidateCsvLine


  override protected def insertRecordsIntoDb(seqRecords: Seq[Any],
                                             dstGtfsDb: DbGtfs) {

    val seqBusTrips = seqRecords.asInstanceOf[Iterable[DbSchemaTrip]]

    val insert = DBIO.seq(dstGtfsDb.busTrips ++= seqBusTrips).transactionally

    // dstGtfsDb.db.run(insert)
    Await.ready(dstGtfsDb.db.run(insert), Duration.Inf)
  }

} // class ConvertBusTripsCsvToDbTable

