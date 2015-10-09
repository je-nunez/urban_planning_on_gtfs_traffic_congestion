package src.main.scala.db

/*
 * class: DbGtfs
 *
 * This class contains all the GTFS SQL DB related tables.
 * Please see the "jdbcDrvr" value below for the JDBC driver to create and
 * connect to this SQL database.
 */

/*
 * See notes on synchronization below
 */
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import slick.driver.SQLiteDriver.api._

import src.main.scala.config.Config
import src.main.scala.logging.Logging._


class DbGtfs(val destinationDir: String) {

  val jdbcDrvr = "org.sqlite.JDBC"  // This version is using a SQLite DB

  var db: Database = null

  lazy val busRoutes = TableQuery[DbGtfsTableRoutes]

  lazy val busStops = TableQuery[DbGtfsTableStops]

  lazy val busRouteShapes = TableQuery[DbGtfsTableRouteShapes]

  lazy val busTrips = TableQuery[DbGtfsTableTrips]

  lazy val busStopTimes = TableQuery[DbGtfsTableStopTimes]

  lazy val tripSegmentTimes = TableQuery[DbGtfsTableSegmentTime]


  // instance constructor: it creates the SQL DB and all its table schemas
  {
    createTables(Config.gtfsDbFilename)
  }

  def createTables(dbFileName: String) {

    db = Database.forURL(s"jdbc:sqlite:$destinationDir/$dbFileName", jdbcDrvr)

    val createGtfsTables = DBIO.seq(
      (busRoutes.schema ++ busStops.schema ++ busRouteShapes.schema ++
       busTrips.schema ++ busStopTimes.schema).create
    )

    // FIXME: It might be necessary to synchronize the promise below on
    // futures (although there could be other concurrent work on parsing GTFS,
    // etc, while this SQL DDL commands finish)
    Await.ready(db.run(createGtfsTables), Duration.Inf)
    // db.run(createGtfsTables)
  }

  /*
   * Below are Scala Slick classes representing the SQL DB tables minimally
   * equivalent to the General Transit Feed Specification (GTFS) CSV files.
   * (ie., these Scala Slick classes/SQL tables don't have all the fields
   * the GTFS CSV files have)
   *
   * FIXME:
   * We need to check that the SQL DDL (primary keys, unique keys, string-
   * sizes, etc, correspond exactly to the GTFS specification:
   *
   *   https://en.wikipedia.org/wiki/General_Transit_Feed_Specification
   *
   * Note: We do use the "shapes.txt" GTFS CSV file. So far, we do not use
   *       the "agency.txt" GTFS CSV file. We will use "calendar.txt" at
   *       some point, but some transit agencies omit some of its fields.
   */

  /* A SQL table minimally equivalent to the GTFS "routes.txt" CSV table */

  class DbGtfsTableRoutes(tag: Tag) extends
    Table[DbSchemaRoute](tag, "routes") {
      def routeId = column[String]("routeId", O.PrimaryKey)
      def routeShortName = column[String]("routeShortName")
      def routeLongName = column[String]("routeLongName")

      def * = (routeId, routeShortName, routeLongName) <>(
                                                       DbSchemaRoute.tupled,
                                                       DbSchemaRoute.unapply
                                                     )
    }


  /* A SQL table minimally equivalent to the GTFS "stops.txt" CSV table */

  class DbGtfsTableStops(tag: Tag) extends
    Table[DbSchemaStop](tag, "stops") {
      def stopId = column[String]("stopId", O.PrimaryKey)
      def stopName = column[String]("stopName")
      def stopLatitude = column[Double]("stopLatitude")
      def stopLongitude = column[Double]("stopLongitude")

      def * = (stopId, stopName, stopLatitude, stopLongitude) <>(
                                                         DbSchemaStop.tupled,
                                                         DbSchemaStop.unapply
                                                       )
    }


  /* A SQL table minimally equivalent to the GTFS "shapes.txt" CSV table */

  class DbGtfsTableRouteShapes(tag: Tag) extends
    Table[DbSchemaRouteShape](tag, "shapes") {
      def shapeId = column[String]("shapeId")
      def shapePtSequencePos = column[Int]("shapePtSequencePos")

      def shapePtLatitude = column[Double]("shapePtLatitude")
      def shapePtLongitude = column[Double]("shapePtLongitude")

      def pk = primaryKey("pk_shape_id_seq_pos", (shapeId, shapePtSequencePos))

      def * = (shapeId, shapePtSequencePos,
               shapePtLatitude, shapePtLongitude) <>(
                                                  DbSchemaRouteShape.tupled,
                                                  DbSchemaRouteShape.unapply
                                                )
    }


  /* A SQL table minimally equivalent to the GTFS "trips.txt" CSV table */

  class DbGtfsTableTrips(tag: Tag) extends
    Table[DbSchemaTrip](tag, "trips") {
      def routeId = column[String]("routeId")
      def serviceId = column[String]("serviceId")
      def tripId = column[String]("tripId", O.PrimaryKey)
      def directionId = column[Int]("directionId")
      def shapeId = column[String]("shapeId")
      def tripHeadSign = column[String]("tripHeadSign")

      def fkRoutes = foreignKey("fk_trips_routes",
                                routeId, busRoutes)(_.routeId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)
      def fkShapes = foreignKey("fk_trips_shapes",
                                shapeId, busRouteShapes)(_.shapeId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)

      def * = (routeId, serviceId, tripId,
               directionId, shapeId, tripHeadSign) <>(
                                                  DbSchemaTrip.tupled,
                                                  DbSchemaTrip.unapply
                                                )
    }


  /* A SQL table minimally equivalent to the GTFS "stop_times.txt" CSV table */

  class DbGtfsTableStopTimes(tag: Tag) extends
    Table[DbSchemaStopTime](tag, "stop_times") {
      def tripId = column[String]("tripId")
      def arrivalTime = column[Int]("arrivalTime")
      def departureTime = column[Int]("departureTime")
      def stopId = column[String]("stopId")
      def stopSequenceNumb = column[Int]("stopSequenceNumb")
      def pickupType = column[String]("pickupType")
      def dropOffType = column[String]("dropOffType")

      def fkTrips = foreignKey("fk_times_trips",
                                tripId, busTrips)(_.tripId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)
      def fkStops = foreignKey("fk_times_stops",
                                stopId, busStops)(_.stopId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)

      def * = (tripId, arrivalTime,
               departureTime, stopId,
               stopSequenceNumb,
               pickupType,
               dropOffType) <> (
                               DbSchemaStopTime.tupled,
                               DbSchemaStopTime.unapply
                             )
    }


  /* A SQL table, "segment_times", derived from the GTFS "stop_times.txt" table */

  class DbGtfsTableSegmentTime(tag: Tag) extends
    Table[DbSchemaSegmentTime](tag, "segment_times") {

      def tripId = column[String]("tripId")
      def stopIdDeparture = column[String]("stopIdDeparture")
      def stopIdArrival = column[String]("stopIdArrival")
      def departStopSequenceNumb = column[Int]("departStopSequenceNumb")
      def departureTime = column[Int]("departureTime")
      def arrivalTime = column[Int]("arrivalTime")
      def delayTime = column[Int]("delayTime", O.Default(-1))

      def fkTrips = foreignKey("fk_segments_trips",
                                tripId, busTrips)(_.tripId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)
      def fkDepartStops = foreignKey("fk_segments_depart_stops",
                                stopIdDeparture, busStops)(_.stopId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)
      def fkArrivalStops = foreignKey("fk_segments_arrival_stops",
                                stopIdArrival, busStops)(_.stopId,
                                        onUpdate = ForeignKeyAction.Cascade,
                                        onDelete = ForeignKeyAction.Cascade)

      def * = (tripId, stopIdDeparture,
               stopIdArrival,
               departStopSequenceNumb,
               departureTime, arrivalTime,
               delayTime) <> (
                               DbSchemaSegmentTime.tupled,
                               DbSchemaSegmentTime.unapply
                             )
    }

}
