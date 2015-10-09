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
import src.main.scala.db.{DbGtfs, DbSchemaStopTime}


class ConvertBusStopTimesCsvToDbTable(
     srcGtfsBusStopTimesCsvFName: String,
     gtfsBusStopTimesConfigOpts: ConfigPerGtfsCsvFile
  ) extends ConvertGtfsCsvToDbTable(
     srcGtfsBusStopTimesCsvFName,
     gtfsBusStopTimesConfigOpts
  ) {

  override def transformCsvHeaderLine(cvs_hdr_line: String): Array[String] =
    cvs_hdr_line.split(",").map(_.trim)

  /*
   * constructor this(srcGtfsCsvDirectory)
   *
   * An auxiliary constructor which assumes that the GTFS Bus Stop Times
   * CSV file, and the destination WEKA Binary serialized instances will be
   * in the same GTFS directory.
   */

  def this(srcGtfsCsvDirectory: String) =
    this(srcGtfsCsvDirectory + "/" + Config.gtfsBusStopTimesInputCsv,
         new ConfigPerGtfsCsvFile(Config.parserGtfsCsv.
                                               minRequiredFieldsBusStopTimes)
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
          logMsg(WARNING, "Ignoring: wrong CSV stop_time: " + dataLine)
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

    // Validate that the "arrival_time" and "departure_time" fields of the
    // GTFS "stop_times.txt" files is a time in the format "H:mm:ss"

    val colNumberArrivalT = posRequiredHeaderFields("arrival_time")
    val colNumberDepartureT = posRequiredHeaderFields("departure_time")

    var timeArrival: Int = -100000000
    var timeDeparture: Int = timeArrival   // initialization before parsing
    try {
      timeArrival = ConvertTimeToSeconds(lineValues(colNumberArrivalT))
      timeDeparture = ConvertTimeToSeconds(lineValues(colNumberDepartureT))
    } catch {
      case e: java.text.ParseException => {
          logMsg(WARNING, "Ignoring: wrong time in CSV line: " + dataLine)
          return null
        }
    }

    // Validate that, in this CSV line, "arrival_time" is <= "departure_time"
    if (timeArrival > timeDeparture) {
      logMsg(WARNING, "Ignoring: arrival time is posterior to departure " +
                       "time in CSV line: " + dataLine)
      return null
    }

    // clean these two fields "arrival_time" and "departure_time" and put
    // instead of the "H:m:s", just the seconds of this "H:m:s"
    lineValues(colNumberArrivalT) = timeArrival.toString
    lineValues(colNumberDepartureT) = timeDeparture.toString

    // We validate that the field "stop_sequence" with the order of stops in
    // a trip be a non-negative number (ie., that "stop_sequence" >= 0)
    val posStopSequence = posRequiredHeaderFields("stop_sequence")
    val strStopSequence = lineValues(posStopSequence)
    var intStopSequence: Int = -1
    try {
      intStopSequence = strStopSequence.toInt
    } catch {
      case e: NumberFormatException => {
                 logMsg(WARNING, "Ignoring: not an integer in " +
                          (posStopSequence + 1) + " column: value: '" +
                          strStopSequence + "'; line: " + dataLine)
                 return null
              }
    }

    if (intStopSequence < 0) {
      logMsg(WARNING, "Ignoring: not a non-negative int in " +
                       (posStopSequence + 1) + " field: value: " +
                       intStopSequence + "; line: " +
                       dataLine)
      return null
    }

    // Return the SQL record (Scala case class) with our required fields in
    // this GTFS CSV line (the other fields in this GTFS CSV line that we
    // don't require are ignored and not written to the database)

    val fieldsToCols = posRequiredHeaderFields
    val tripId = lineValues(fieldsToCols("trip_id"))
    val stopId = lineValues(fieldsToCols("stop_id"))
    val pickupType = lineValues(fieldsToCols("pickup_type"))
    val dropOffType = lineValues(fieldsToCols("drop_off_type"))

    logMsg(DEBUG, s"Valid bus stop time: $tripId, $stopId at $timeArrival")
    return DbSchemaStopTime(tripId, timeArrival, timeDeparture, stopId,
                            intStopSequence, pickupType, dropOffType)

  } // method parseAndValidateCsvLine

  override protected def insertRecordsIntoDb(seqRecords: Seq[Any],
                                             dstGtfsDb: DbGtfs) {

    val seqBusStopTimes = seqRecords.asInstanceOf[Iterable[DbSchemaStopTime]]

    val insert = DBIO.seq(dstGtfsDb.busStopTimes ++= seqBusStopTimes).transactionally

    // dstGtfsDb.db.run(insert)
    Await.ready(dstGtfsDb.db.run(insert), Duration.Inf)
  }

} // class ConvertBusStopTimesCsvToDbTable

