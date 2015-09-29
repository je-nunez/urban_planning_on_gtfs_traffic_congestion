package src.main.scala.db

/*
 * object: DbSchemaRouteShape
 *
 * The record schema of a single bus "route shape" in the "shapes.txt" CSV
 * file in the General Transit Feed Specification (GTFS)
 */

case class DbSchemaRouteShape(shapeId: String, shapePtSequencePos: Int,
                         shapePtLatitude: Double, shapePtLongitude: Double)

