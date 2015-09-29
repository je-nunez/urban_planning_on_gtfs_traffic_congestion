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
import src.main.scala.db.{DbGtfs, DbSchemaRouteShape}


class ConvertRouteShapesCsvToDbTable(
     srcGtfsBusRouteShapesCsvFName: String,
     gtfsBusRouteShapesConfigOpts: ConfigPerGtfsCsvFile
  ) extends ConvertGtfsCsvToDbTable(
     srcGtfsBusRouteShapesCsvFName,
     gtfsBusRouteShapesConfigOpts
  ) {

  override def transformCsvHeaderLine(cvs_hdr_line: String): Array[String] =
    cvs_hdr_line.split(",").map(_.trim)

  /*
   * constructor this(srcGtfsCsvDirectory)
   *
   * An auxiliary constructor which assumes that the GTFS Bus Route Shapes
   * CSV file, and the destination WEKA Binary serialized instances will be
   * in their default location.
   */

  def this(srcGtfsCsvDirectory: String) =
    this(srcGtfsCsvDirectory + "/" + Config.gtfsRouteShapesInputCsv,
         new ConfigPerGtfsCsvFile(Config.parserGtfsCsv.
                                              minRequiredFieldsRouteShapes)
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
          logMsg(WARNING, "Ignoring: wrong CSV shape: " + dataLine)
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

    // Validate that the "shape_pt_lat" and "shape_pt_lon" have a valid point

    val colNumberShapeLatit = posRequiredHeaderFields("shape_pt_lat")
    val colNumberShapeLongit = posRequiredHeaderFields("shape_pt_lon")

    var stopGeom: jts.geom.Geometry = null
    try {
      stopGeom = StringToGeom(
                               lineValues(colNumberShapeLongit) + "," +
                               lineValues(colNumberShapeLatit) + " "
                             )
    } catch {
      case e: java.text.ParseException => {
          logMsg(WARNING, "Ignoring: wrong time in CSV line: " + dataLine)
        return null
      }
    }

    // We validate that the field "shape_pt_sequence" is an integer >= 0
    val posShapePointSeqNumb = posRequiredHeaderFields("shape_pt_sequence")
    val strShapePointSeqNumb = lineValues(posShapePointSeqNumb)
    var intShapePointSeqNumb: Int = -1000000
    try {
      intShapePointSeqNumb = strShapePointSeqNumb.toInt
    } catch {
      case e: NumberFormatException => {
                 logMsg(WARNING, "Ignoring: not an integer in " +
                          (posShapePointSeqNumb + 1) + " column: value: '" +
                          strShapePointSeqNumb + "'; line: " + dataLine)
                 return null
              }
    }

    if (intShapePointSeqNumb < 0) {
      logMsg(WARNING, "Ignoring: not a non-negative int in " +
                       (posShapePointSeqNumb + 1) + " field: value: " +
                       intShapePointSeqNumb + "; line: " +
                     dataLine)
      return null
    }

    // Return the SQL record (Scala case class) with our required fields in
    // this GTFS CSV line (the other fields in this GTFS CSV line that we
    // don't require are ignored and not written to the database)

    val fieldsToCols = posRequiredHeaderFields
    val shapeId = lineValues(fieldsToCols("shape_id"))
    val shapePtLatitude = lineValues(colNumberShapeLatit).toDouble
    val shapePtLongitude = lineValues(colNumberShapeLongit).toDouble

    logMsg(DEBUG, s"Valid shape: $shapeId, $intShapePointSeqNumb ...")
    return DbSchemaRouteShape(shapeId, intShapePointSeqNumb, shapePtLatitude,
                              shapePtLongitude)

  } // method parseAndValidateCsvLine


  override protected def insertRecordsIntoDb(seqRecords: Seq[Any],
                                             dstGtfsDb: DbGtfs) {

    val seqShapes = seqRecords.asInstanceOf[Iterable[DbSchemaRouteShape]]

    val insert = DBIO.seq(dstGtfsDb.busRouteShapes ++= seqShapes).transactionally

    // dstGtfsDb.db.run(insert)
    Await.ready(dstGtfsDb.db.run(insert), Duration.Inf)
  }

} // class ConvertRouteShapesCsvToDbTable

