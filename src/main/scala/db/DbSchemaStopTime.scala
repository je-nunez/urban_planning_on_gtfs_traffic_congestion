package src.main.scala.db

/*
 * object: DbSchemaStopTime
 *
 * The record schema of a single bus "stop time" in the "stop_times.txt" CSV
 * file in the General Transit Feed Specification (GTFS)
 *
 * Note that there is an extra field in the record, "delayTime: Int", not
 * specified in the GTFS "stop_times.txt" CSV: this value is the delay in
 * arriving to ___this___ stop from the previous stop in the same tripId,
 * ie., the previous stop is the one have (stopSequenceNumb - 1).
 */

case class DbSchemaStopTime(tripId: String,
                            arrivalTime: Int, departureTime: Int,
                            stopId: String, stopSequenceNumb: Int,
                            pickupType: String, dropOffType: String,
                            delayTime: Int)

