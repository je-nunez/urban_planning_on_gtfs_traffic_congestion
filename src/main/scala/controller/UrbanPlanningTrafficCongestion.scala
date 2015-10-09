package src.main.scala.controller

import scala.collection.JavaConversions._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import slick.driver.SQLiteDriver.api._

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.db.DbGtfs
import src.main.scala.opendata._

class UrbanPlanningTrafficCongestion(
     val srcGtfsCvsDirectory: String,
     val dstGtfsDb: DbGtfs
  ) {


  def loadGtfsModelIntoDb(beLaxValidatingTransitAgencies: Boolean) {

    // Convert the Bus-Stops geometries in the GTFS CSV file

    val busStops = new ConvertBusStopsCsvToDbTable(srcGtfsCvsDirectory)

    busStops.etlGtfsCsvToDbTable(dstGtfsDb)

    // Convert the Bus-Routes in the GTFS CSV file

    val busRoutes = new ConvertRoutesCsvToDbTable(srcGtfsCvsDirectory)

    busRoutes.etlGtfsCsvToDbTable(dstGtfsDb, beLaxValidatingTransitAgencies)

    // Convert the Route-Shapes geometries in the GTFS CSV file

    val routeShapes = new ConvertRouteShapesCsvToDbTable(srcGtfsCvsDirectory)

    routeShapes.etlGtfsCsvToDbTable(dstGtfsDb)

    // Convert the Bus-Trips geometries in the GTFS CSV file

    val busTrips = new ConvertBusTripsCsvToDbTable(srcGtfsCvsDirectory)

    busTrips.etlGtfsCsvToDbTable(dstGtfsDb)

    // Convert the Bus-Stop Times in GTFS

    val busStopTimes = new ConvertBusStopTimesCsvToDbTable(srcGtfsCvsDirectory)

    busStopTimes.etlGtfsCsvToDbTable(dstGtfsDb)

  } // end of method loadGtfsModel()


  // The name of this method "calculateTripSegmentTimes" is reminiscent
  // from the New York City's Department of City Planning LION Single-Line
  // Street ___Segments___, although the LION Street Segments are __minimal__
  // segments, here the segments are between consecutive transit stops, ie.,
  // they needn't be minimal from the Department of City Planning's
  // perspective, but they are the ones whose delay-times and congestion can
  // be inferred from the Transit and Traffic datasources.

  def calculateTripSegmentTimes() {

    // create the "segment_times" SQL table
    val createSegmentTimesTable = DBIO.seq(
       dstGtfsDb.tripSegmentTimes.schema.create
    )
    Await.ready(dstGtfsDb.db.run(createSegmentTimesTable), Duration.Inf)

    // create a new index on the GTFS "stop_times" SQL table, since it will help
    // us to speed up the calculation of the delay between successive stops
    // in a same trip (the successive stops in a same tripId are those with
    // consecutive values of stopSequenceNumb)

    val newIdxStopTimes =
      sqlu"""CREATE UNIQUE INDEX idx_trip_stopseqnumb
                    ON stop_times (tripId, stopSequenceNumb)"""

    Await.ready(dstGtfsDb.db.run(newIdxStopTimes), Duration.Inf)

    // update the SQL values histogram for this new index on that table, for
    // the SQLite documentation says: https://www.sqlite.org/lang_analyze.html
    //
    //   "Statistics gathered by ANALYZE are not automatically updated as the
    //   content of the database changes. If the content of the database
    //   changes significantly, or if the database schema changes, then one
    //   should consider rerunning the ANALYZE command in order to update the
    //   statistics."

    val analyzeHistogramIdx = sqlu"""ANALYZE idx_trip_stopseqnumb"""
    Await.ready(dstGtfsDb.db.run(analyzeHistogramIdx), Duration.Inf)

    // calculate into the new table "segment_times", the delay between
    // successive stops in a same trip, for all the trips (the index created
    // in the instruction before speeds up this calculation)

    val insertTripSegmentDelays =
      sqlu"""INSERT INTO segment_times
                SELECT t1.tripId, t1.stopId, t2.stopId, t1.stopSequenceNumb,
                       t1.departureTime, t2.arrivalTime,
                       t2.arrivalTime - t1.departureTime
                FROM stop_times t1 INNER JOIN stop_times t2
                     ON t1.tripId = t2.tripId AND
                        t1.stopSequenceNumb = t2.stopSequenceNumb - 1"""

    Await.ready(dstGtfsDb.db.run(insertTripSegmentDelays), Duration.Inf)

  }

/*
   Other queries on the new, non-GTFS table "segment_times":

        SELECT "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb", COUNT(*) as trip_numbers, MIN(delayTime) as min_trip, AVG(delayTime), MAX(delayTime) as max_trip FROM segment_times GROUP BY "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb" H,

        INSERT INTO segment_times SELECT t1.tripId, t1.stopId, t2.stopId, t1.stopSequenceNumb, t1.departureTime, t2.arrivalTime, t2.arrivalTime - t1.departureTime from stop_times t1 inner join stop_times t2 on t1.tripId = t2.tripId and t1.stopSequenceNumb = t2.stopSequenceNumb - 1;

        SELECT "tripId", "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb", COUNT(*) as trip_numbers, MIN(delayTime), AVG(delayTime), MAX(delayTime) FROM segment_times GROUP BY "tripId", "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb" HAVING trip_numbers > 10;

        SELECT "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb", COUNT(*) as trip_numbers, MIN(delayTime), AVG(delayTime), MAX(delayTime) FROM segment_times GROUP BY "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb" HAVING trip_numbers > 10;

        SELECT "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb", COUNT(*) as trip_numbers, MIN(delayTime) as min_trip, AVG(delayTime), MAX(delayTime) as max_trip FROM segment_times GROUP BY "stopIdDeparture", "stopIdArrival", "departStopSequenceNumb" HAVING trip_numbers > 10 and max_trip - min_trip > 20;

        SELECT * FROM segment_times WHERE stopIdDeparture = 10009 and stopIdArrival = 10299 and departStopSequenceNumb = 27 ;

        SELECT tripId, stopIdDeparture, departureTime / 3600 as hour, delayTime FROM segment_times WHERE stopIdDeparture = 10009 and stopIdArrival = 10299 and departStopSequenceNumb = 27  ORDER BY hour ;

*/

} // end of class UrbanPlanningTrafficCongestion

