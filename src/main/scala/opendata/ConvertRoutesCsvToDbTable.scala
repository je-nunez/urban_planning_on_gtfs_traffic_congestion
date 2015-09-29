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
import src.main.scala.db.{DbGtfs, DbSchemaRoute}


class ConvertRoutesCsvToDbTable(
     srcGtfsBusRoutesCsvFName: String,
     gtfsBusRoutesConfigOpts: ConfigPerGtfsCsvFile
  ) extends ConvertGtfsCsvToDbTable(
     srcGtfsBusRoutesCsvFName,
     gtfsBusRoutesConfigOpts
  ) {

  // Whether the "routes.txt" GTFS CSV file has an "agency_id" field or not,
  // and if so, in what column number is this field at in this file
  // (we don't say that "agency_id" is a required field for the "routes.csv"
  // GTFS file -eg., Ottawa OC Transit don't give it in its "routes.csv"
  // file-, but we, internally, if this field exists, then we check that this
  // "routes.csv" file has only one value for the "agency_id" field)

  protected [this] var agencyIdColumnNumber: Int = -1

  // This is the value of the Agency_Id that we found in the first data line
  // of this "routes.txt" GTFS CSV file, and that we then use to ensure that
  // all the next data lines in this "routes.txt" GTFS CSV file has the same
  // value of the Agency_Id

  protected [this] var firstAgencyId: String = null

  // Whether to validate or not that the "routes.txt" GTFS file is for a
  // unique Transit Agency-value, or to allow this file to contain multiple
  // Transit Agency-values (and if so, to ignore this fact)

  protected [this] var validateUniqueAgencyValue: Boolean = true

  /*
   * constructor this(srcGtfsCsvDirectory)
   *
   * An auxiliary constructor which assumes that the GTFS Bus Routes CSV file,
   * stination WEKA Binary serialized instances will be in their default
   * location.
   */

  def this(srcGtfsCsvDirectory: String) =
    this(srcGtfsCsvDirectory + "/" + Config.gtfsRoutesInputCsv,
         new ConfigPerGtfsCsvFile(Config.parserGtfsCsv.
                                              minRequiredFieldsRoutes)
        )

  override def transformCsvHeaderLine(cvsHeaderLine: String): Array[String] =
    {
      val cvsHeaderFields = cvsHeaderLine.split(",").map(_.trim)

      // set our internal index on whether the header of this "routes.txt"
      // CSV file declares an "agency_id" field, and on what position this
      // CSV column is
      agencyIdColumnNumber = cvsHeaderFields.indexOf("agency_id")
      logMsg(DEBUG, "Position of 'agency_id' column in this 'routes.csv' " +
                     "file: " + agencyIdColumnNumber +
                     " header-line: " + cvsHeaderLine)

      cvsHeaderFields
    }

  /**
    * this method: <code>parseAndValidateCsvLine</code>
    *
    * Check if the CSV data-line from the GTFS CSV has all the fields
    * that we expect and in the format we expect them, and transform it,
    * returning the array of values for those fields: otherwise, ignore
    * the bad input record, returning null in such a case.
    */

  protected def parseAndValidateCsvLine(dataLine: String): Any = {

    // a better parser is needed for CSV lines with values with ","

    var lineValues: Array[String] = null
    try {
      lineValues = dataLine.split(",").map(_.trim)
    } catch {
      case e: java.util.NoSuchElementException => {
          logMsg(WARNING, "Ignoring: wrong CSV route: " + dataLine)
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
  
    // We only validate that, if the "agency_id" field exists in this
    // "routes.csv" GTFS file, then it has the same value for all the records
    // (do we need this? can be we lax in this and assume that, even if
    // different agencies exist in this "routes.csv" file, then they will
    // collaborate with each other agency and have different "route_id" and
    // route names?
  
    if (validateUniqueAgencyValue && agencyIdColumnNumber >= 0 &&
        ! checkAgencyIdValue(lineValues, dataLine))
      return null
  
    // Return the SQL record (Scala case class) with our required fields in
    // this GTFS CSV line (the other fields in this GTFS CSV line that we
    // don't require are ignored and not written to the database)
  
    val fieldsToCols = posRequiredHeaderFields
    val routeId = lineValues(fieldsToCols("route_id"))
    val routeShortName = lineValues(fieldsToCols("route_short_name"))
    val routeLongName = lineValues(fieldsToCols("route_long_name"))
  
    logMsg(DEBUG, s"Valid route: $routeId, $routeShortName, $routeLongName")
    return DbSchemaRoute(routeId, routeShortName, routeLongName)

  } // method parseAndValidateCsvLine


  protected def checkAgencyIdValue(lineValues: Array[String],
                                   originalDataLine: String): Boolean =
  {
    if (agencyIdColumnNumber >= lineValues.length) {
      // this line doesn't have a value for the "agency_id" column
      logMsg(WARNING, "Ignoring: doesn't have right number of fields: " +
                       "(missing value for 'agency_id' column): " +
                       originalDataLine)
      return false
    } else {
      // this line does have a value for the "agency_id" column
      val agencyId = lineValues(agencyIdColumnNumber)

      if (firstAgencyId == null) {
         firstAgencyId = agencyId
       } else if (agencyId != firstAgencyId) {
         // The CSV "routes.txt" file has two different transit agencies
         logMsg(WARNING, "Ignoring: a different transit agency '" +
                          agencyId + "': expected: '" + firstAgencyId +
                          "' in CSV line: " + originalDataLine)
         return false
       }
    } // end of "else" of "if (agencyIdColumnNumber >= lineValues.length)"

    true  // this line is ok
  }


  def etlGtfsCsvToDbTable(dstGtfsDb: DbGtfs,
                          beLaxValidatingTransitAgencies: Boolean) {

      // we validate that _all_ the routes in this GTFS routes CSV file belong
      // to the _same and one_ agency, except when we are asked to be lax and
      // assume that route_ids R[i] and R[j] are different for all lines [i]
      // and [j], even if they have different transit agencies. Ie., that even
      // if there are different transit agencies in this GTFS routes.txt CSV
      // file, all agencies have agreed among them to have different
      // route_ids, ie., two different agencies don't show to their clients
      // a common route_id "1", since that would be confusing to the their
      // clients (ie., we are not thinking in transit subcontractors)

      validateUniqueAgencyValue = (! beLaxValidatingTransitAgencies)

      etlGtfsCsvToDbTable(dstGtfsDb)
  }


  override protected def insertRecordsIntoDb(seqRecords: Seq[Any],
                                             dstGtfsDb: DbGtfs) {

    val seqBusRoutes = seqRecords.asInstanceOf[Iterable[DbSchemaRoute]]

    val insert = DBIO.seq(dstGtfsDb.busRoutes ++= seqBusRoutes).transactionally

    // dstGtfsDb.db.run(insert)
    Await.ready(dstGtfsDb.db.run(insert), Duration.Inf)
  }

} // class ConvertRoutesCsvToDbTable
