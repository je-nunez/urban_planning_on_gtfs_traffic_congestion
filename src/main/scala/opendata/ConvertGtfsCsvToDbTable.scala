
package src.main.scala.opendata

import _root_.java.util.Date
import _root_.java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import slick.driver.SQLiteDriver.api._

import src.main.scala.logging.Logging._
import src.main.scala.config.ConfigPerGtfsCsvFile
import src.main.scala.db.DbGtfs

/**
  * abstract class: <code>ConvertGtfsCsvToDb</code>
  *
  * This is the ETL __base__ class (Extract-Transform-Load). Its purpose is
  * to open a GTFS CSV file, to read and transform it, and then to insert the
  * result into a table in a SQL database. (This is the ETL logic: for the
  * SQL database, see the class <code>DbGtfs</code>)
  */

abstract class ConvertGtfsCsvToDbTable(
     val srcFNameGtfsCsv: String,
     val gtfsConfigOptions: ConfigPerGtfsCsvFile
  ) {

  protected [this] lazy val reqCvsFields =
    gtfsConfigOptions.minRequiredCsvFields.view

  /**
    * this field: <code>posRequiredHeaderFields</code>
    *
    * given the subset of GTFS field names that our application is interested
    * in this GTFS CSV file, this field <code>posRequiredHeaderFields</code>
    * is a mapping of those interesting field names to their actual column
    * postions as they are really arranged in this GTFS CSV file.
    *
    * (Ie., column positions for a field name needn't be fixed in the
    * General Transit Feed Specification, e.g., the "route_id" field needn't
    * be always at the first column. But other parts in our system
    * may require fixed, well-known column positions for different fields)
    */

  protected [this] var posRequiredHeaderFields: Map[String, Int] = null


  /**
    * this field: <code>maxRequiredColumn</code>
    *
    * An optimization: once the actual mapping
    * <code>posRequiredHeaderFields</code> of field names into column indexes
    * is known for this actual GTFS CSV file, then what is the minimum number
    * of colums that the following data lines in this GTFS CSV file must have
    * to be valid for this ETL on this GTFS CSV file.
    */

  protected [this] lazy val maxRequiredColumn =
    posRequiredHeaderFields.values.max


  /**
    * this method: <code>leaveOnlyRequiredFields</code>
    *
    * Projects an input array with all the field values in a GTFS CSV line,
    * onto only those GTFS fields our application is interested in, discarding
    * all the other GTFS fields not interesting.
    *
    * To do this projection, it uses the mapping
    * <code>posRequiredHeaderFields</code> of the GTFS fields our application
    * is interested in, into their corresponding column numbers (array indexes)
    * as they happened to appear in this GTFS CSV file.
    *
    * @param fieldValues array with all the fields (values) of a line in the
    *                    original GTFS CSV file, even of those fields of the
    *                    GTFS CSV file our application doesn't need to process
    * @return            the array with only the values of those fields our
    *                    application wants to process, so all other fields are
    *                    ignored.
    */

  protected [this] def leaveOnlyRequiredFields(fieldValues: Array[String]):
    Array[String] = {
      posRequiredHeaderFields.values.toArray.sorted map(i => fieldValues(i))
    }


  /**
    * this abstract method: <code>transformCsvHeaderLine</code>
    *
    * Receives the CSV header line and transforms it into an array of field
    * names present in this line.
    *
    * @param csvHdrLine  the string with the CSV header line
    * @return            the array with the field names present in this CSV
    *                    header line.
    */

  protected def transformCsvHeaderLine(csvHdrLine: String): Array[String]

  /**
    * this method: <code>buildMappingRequiredFieldsToColumnIndexes</code>
    *
    * Check if the CSV header from the OpenData CSV URL is the same as we
    * expected, because if the CSV header has changed, then the parser
    * needs to change
    *
    * @param headerLine the CSV header line from the CSV URL to check
    */

  protected def buildMappingRequiredFieldsToColumnIndexes(headerLine: String) {

    // With the CSV header line, we always leave only the printable ASCII
    // chars since the General Transit Feed Specification only gives ASCII
    // chars for every GTFS CSV header line (for data lines in the CSV,
    // given by the actual transit agencies, there could be non-ASCII
    // characters)

    val cleanHdrLine = headerLine.replaceAll("[^\\p{InBasicLatin}]","").
                        replaceAll("[\\p{C}]","")

    if (headerLine != cleanHdrLine)
       logMsg(WARNING, "Header line for GTFS CVS file '" + srcFNameGtfsCsv +
                       "' had to be cleaned up.")

    var gtfsFields: Array[String] = transformCsvHeaderLine(cleanHdrLine)

    /* Check if these fields in this GTFS header line contain at least
     * our minimum expected gtfsFields.
     */

    logMsg(DEBUG, "received GTFS fields: " + gtfsFields.mkString(","))
    logMsg(DEBUG, "required minimum set of GTFS fields: " +
                   gtfsConfigOptions.minRequiredCsvFields.mkString(","))

    // Find in what columns in the GTFS CVS file our required fields appear
    val mapReqFieldsToActualColumns =
      (reqCvsFields map {
                         reqField => reqField -> gtfsFields.indexOf(reqField)
                        }
      ).toMap

    // Check if this actual GTFS CVS file is missing some of our required
    // fields, in which case we would raise an exception
    var missingReqFields: Boolean = false

    mapReqFieldsToActualColumns.foreach {
      case(cvsField, columnPos) => if (columnPos == -1) {
          logMsg(EMERGENCY,
                 s"Required field '$cvsField' not found in GTFS CSV header."
                )
          missingReqFields = true
        }
    }

    if (missingReqFields) {
        logMsg(EMERGENCY, "Format of the GTFS CSV header fields in file '" +
                           srcFNameGtfsCsv + "' has changed:"  +
                           "\n   Found header: " + gtfsFields.mkString(",")
               )
        throw new Exception("Format of the GTFS CSV header in '" +
                            srcFNameGtfsCsv + "' has changed.")
    }

    posRequiredHeaderFields = mapReqFieldsToActualColumns
  } // end buildMappingRequiredFieldsToColumnIndexes


  /**
    * this abstract method: <code>parseAndValidateCsvLine</code>
    *
    * Check if the CSV data-line from the GTFS CSV has all the fields
    * that we expect and in the format we expect them, and transform it,
    * returning the array of values for those fields: otherwise, ignore
    * the bad input record, returning null in such a case.
    *
    * (This method returns the array with the values of the fields: the
    * mapping between the field names and the array indexes are given by the
    * field <code>posRequiredHeaderFields</code> above.)
    */

  protected def parseAndValidateCsvLine(dataLine: String): Any


  protected def insertRecordsIntoDb(seqRecords: Seq[Any], dstGtfsDb: DbGtfs)


  def etlGtfsCsvToDbTable(dstGtfsDb: DbGtfs) {

    // open the GTFS CSV file and do an ETL on it into the SQL DB dstGtfsDb

    val recordsToInsert = new ArrayBuffer[Any]()
    var srcCsv: scala.io.BufferedSource = null

    try {
      srcCsv = io.Source.fromFile(srcFNameGtfsCsv)
      var lineNumber = 1

      for (line <- srcCsv.getLines()) {
        if (lineNumber == 1)
          buildMappingRequiredFieldsToColumnIndexes(line)
        else {
          val record = parseAndValidateCsvLine(line)
          if (record != null)
            recordsToInsert += record
        }
        lineNumber += 1
      }

    } catch {
          case e: java.io.IOException => {
                    logMsg(EMERGENCY, "I/O error occurred: " + e.getMessage)
                    throw e    // rethrow I/O error exception up to caller
            }
    } finally {
      // insert the records into the SQL DB
      if (recordsToInsert.length > 0)
        insertRecordsIntoDb(recordsToInsert, dstGtfsDb)

      // close the source GTFS CSV file
      if (srcCsv != null)
        srcCsv.close
    }

  } // method etlGtfsCsvToSpecificDbTable

}
