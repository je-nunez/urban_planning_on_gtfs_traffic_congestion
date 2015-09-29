package src.main.scala.db

/*
 * object: DbSchemaStop
 *
 * The record schema of a single bus "stop" in the "stops.txt" CSV file in
 * the General Transit Feed Specification (GTFS)
 */

case class DbSchemaStop(stopId: String, stopName: String,
                        stopLatitude: Double, stopLongitude: Double)

/*
 * Notes:
 * About the "wheelchair_boarding" field:
 * some transit agencies, like Vancouver Translink, don't provide this field
 * (we could assume that it is "1" given the high-quality of living of
 * Vancouver)
 */

