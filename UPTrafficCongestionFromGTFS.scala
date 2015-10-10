#!/usr/bin/env scala -deprecation -J-Xmx4g -J-XX:NewRatio=4

// !/usr/bin/env scala -deprecation -J-Xmx4g -J-XX:NewRatio=4 -J-verbose:gc

import _root_.java.util.Calendar
import scala.collection.JavaConversions._

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

/* Internal packages of this project. In this very first version, they
 * have this simple prefix, but will surely change. */

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.controller._
import src.main.scala.db.DbGtfs
import src.main.scala.types.PostalCode
import src.main.scala.geodecoding.GeoDecodingProviderNominatim
import src.main.scala.geodecoding.GeoDecodingProviderGoogleMaps

/*
 *   MAIN PROGRAM
 *      relying all the functionality to the application packages above
 *
 */

object urbanPlanningOnTrafficCongestion {

  type OptionMap = Map[String, String]

  def processCmdLineArgs(map: OptionMap, list: List[String]): OptionMap = {

    def invalidCommandLineOptions(options: List[String]) {
      logMsg(EMERGENCY, "Unknown command-line argument: " +
                         options.mkString(" "))
      sys.exit(1)
    }

    list match {
      case Nil => map
      case "--ignore-agencies" :: tail =>
            processCmdLineArgs(map ++ Map("ignore-agencies" -> "true"), tail)
      case string :: tail if (! map.contains("gtfs-dir")) =>
            processCmdLineArgs(map ++ Map("gtfs-dir" -> string), tail)
      case string :: tail if (! map.contains("result-dir")) =>
            processCmdLineArgs(map ++ Map("result-dir" -> string), tail)
      case aRemainder => {
                  invalidCommandLineOptions(aRemainder)
                  map
                }
    }
  }


  def main(cmdLineArgs: Array[String]) {

    val cmdLineOptions = processCmdLineArgs(Map(),cmdLineArgs.toList)
    logMsg(DEBUG, "Parsed command-line options: " +
                   cmdLineOptions.mkString("\n\t"))

    val download_time = Calendar.getInstance().getTime()

    // In what directory we should find the GTFS CSV files

    var gtfsInputDirect: String = Config.gtfsDefaultInputDir

    if (cmdLineOptions.contains("gtfs-dir"))
      gtfsInputDirect = cmdLineOptions("gtfs-dir")

    var dstOutputDirect: String = gtfsInputDirect  // same input and output

    if (cmdLineOptions.contains("result-dir"))
      dstOutputDirect = cmdLineOptions("result-dir")

    val ignoreAgencyValidation = cmdLineOptions.contains("ignore-agencies")

    // test geo-decoding a latitude-longitude to get its postal code

    GeoDecodingProviderNominatim(40.750556, -73.993611) match {
      case Success(postalCode) => println(postalCode.countryCode + " " +
                                          postalCode.postalCode)
      case Failure(ex) => println(s"An exception occurred: ${ex.getMessage}")
    }

    GeoDecodingProviderGoogleMaps(40.750556, -73.993611) match {
      case Success(postalCode) => println(postalCode.countryCode + " " +
                                          postalCode.postalCode)
      case Failure(ex) => println(s"An exception occurred: ${ex.getMessage}")
    }

    val gtfsDb = new DbGtfs(dstOutputDirect)

    val urbanPlanningController =
      new UrbanPlanningTrafficCongestion(gtfsInputDirect, gtfsDb)

    urbanPlanningController.loadGtfsModelIntoDb(ignoreAgencyValidation)

    urbanPlanningController.calculateTripSegmentTimes()

    urbanPlanningController.findTrafficSegmentsWithHighestStdDeviation()

  }  // end of method main(...)
} // end of object urbanPlanningOnTrafficCongestion

