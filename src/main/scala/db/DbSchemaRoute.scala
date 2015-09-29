package src.main.scala.db

/*
 * object: DbSchemaRoute
 *
 * The record schema of a single bus "route" in the "routes.txt" CSV file in
 * the General Transit Feed Specification (GTFS)
 */

case class DbSchemaRoute(routeId: String, routeShortName: String,
                         routeLongName: String)

/*
 * Notes:
 * About the "agency_id" field:
 * some transit agencies, like Ottawa OC Transit, don't export the
 * "agency_id" field in their "routes" GTFS CSV table, so we laxed it
 */

