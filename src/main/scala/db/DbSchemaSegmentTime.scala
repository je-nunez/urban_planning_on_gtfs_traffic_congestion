package src.main.scala.db

/**
 * case class: DbSchemaSegmentTime
 *
 * The record schema of the delay in seconds of a trip between two
 * of its successive stops. Ie., it has the delay in seconds that the
 * trip took to travel the segment of two successive bus stops in this
 * trip (for each combination of successive bus stops in each trip).
 *
 * This is an analysis table, derived from GTFS "stop_times" but not
 * in the GTFS specification.
 */

case class DbSchemaSegmentTime(tripId: String,
                               stopIdDeparture: String, // StopID of the departure start of segment
                               stopIdArrival: String, // StopID of the arrival end of the segment
                               departStopSequenceNumb: Int, // Sequence Number of Departure in the Trip
                               departureTime: Int,  // as a second offset from beginning of day
                               arrivalTime: Int,
                               delayTime: Int // delay in seconds that took this trip to travel
                                              // this segment from stopIdDeparture to
                                              // stopIdArrival.
                                              // This delay, if defined, is equal to
                                              //
                                              //       ( arrivalTime - departureTime )
                              )

    /*
        CREATE TABLE segment_times(
            "tripId" VARCHAR(254) NOT NULL,
            "stopIdDeparture" VARCHAR(254) NOT NULL,
            "stopIdArrival" VARCHAR(254) NOT NULL,
            "departStopSequenceNumb" INTEGER NOT NULL,
            "departureTime" INTEGER NOT NULL,
            "arrivalTime" INTEGER NOT NULL,
            "delayTime" INTEGER DEFAULT -1 NOT NULL,
            constraint "fk_times_trips" foreign key("tripId")
                   references "trips"("tripId")
                   on update CASCADE on delete CASCADE)
    */

