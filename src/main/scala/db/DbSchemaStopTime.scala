package src.main.scala.db

/*
 * object: DbSchemaStopTime
 *
 * The record schema of a single bus "stop time" in the "stop_times.txt" CSV
 * file in the General Transit Feed Specification (GTFS)
 */

case class DbSchemaStopTime(tripId: String,
                            arrivalTime: Int, departureTime: Int,
                            stopId: String, stopSequenceNumb: Int,
                            pickupType: String, dropOffType: String)

