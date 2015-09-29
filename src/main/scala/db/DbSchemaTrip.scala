package src.main.scala.db

/*
 * object: DbSchemaTrip
 *
 * The record schema of a single bus "trip" in the "trips.txt" CSV file in
 * the General Transit Feed Specification (GTFS)
 */

case class DbSchemaTrip(routeId: String, serviceId: String, tripId: String,
                        directionId: Int, shapeId: String,
                        tripHeadSign: String)

/*
 * Notes:
 * About the "wheelchair_boarding" field:
 * some transit agencies, like Vancouver Translink, don't provide this field
 * (we could assume that it is "1")
 */

