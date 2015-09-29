package src.main.scala.config

/*
 * object: Config
 *
 * It has all the configuration settings
 */

object Config {

  // The default input directory where we expect to find the GTFS CSV files
  // uncompressed

  val gtfsDefaultInputDir = "../gtfs/data"

  val gtfsDbFilename = "gtfs.db"

  // Location for the GTFS Bus "stop_times.txt" CSV file, and other GTFS CSVs

  val gtfsBusStopTimesInputCsv = "stop_times.txt"

  val gtfsBusStopsInputCsv = "stops.txt"

  val gtfsRouteShapesInputCsv = "shapes.txt"

  val gtfsRoutesInputCsv = "routes.txt"

  val gtfsTripsInputCsv = "trips.txt"


  object parserGtfsCsv {

    // parser for GTFS CSV file "routes.txt"

    val minRequiredFieldsRoutes = Array(
                                        "route_id",
                                        // "agency_id",
                                        // some transit agencies, like Ottawa
                                        // OC Transit, don't export the
                                        // "agency_id" field, so we laxed it
                                        "route_short_name",
                                        "route_long_name"
                                       )

    // parser for GTFS CSV file "stops.txt":

    val minRequiredFieldsBusStops = Array(
                                          "stop_id",
                                          "stop_name",
                                          "stop_lat",
                                          "stop_lon"
                                          // "wheelchair_boarding"
                                          // some transit agencies, like
                                          // Vancouver Translink, don't give
                                          // this field (we could assume that
                                          // it is "1")
                                         )

    // parser for GTFS CSV file "shapes.txt": field "shape_dist_traveled" is
    // not required -some transit agencies don't give it, like Ontario GO-
    // and can be calculated, like in

    val minRequiredFieldsRouteShapes = Array(
                                          "shape_id",
                                          "shape_pt_lat",
                                          "shape_pt_lon",
                                          "shape_pt_sequence"
                                         )

    // parser for GTFS CSV file "trips.txt":

    val minRequiredFieldsBusTrips = Array(
                                        "route_id",
                                        "service_id",
                                        "trip_id",
                                        "trip_headsign",
                                        "direction_id",
                                        "shape_id"
                                        // "wheelchair_accessible"
                                        // some agencies, like Vancouver
                                        // Translink, don't give this field
                                        // (we could assume that it is "1")
                                       )

    // parser for GTFS CSV file "stop_times.txt"

    val minRequiredFieldsBusStopTimes = Array(
                                          "trip_id",
                                          "arrival_time",
                                          "departure_time",
                                          "stop_id",
                                          "stop_sequence",
                                          "pickup_type",
                                          "drop_off_type"
                                         )

   } // end of object Config.parserOpenDataCsv

} // end of object Config

