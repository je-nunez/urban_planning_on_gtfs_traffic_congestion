package src.main.scala.utils.conversion

object ConvertTimeToSeconds {

  /*
   * method: convertTimeStrToSeconds
   *
   * Converts a string of the form:
   *
   *      H:m:s
   *
   * where 'H', 'm', and 's' are numbers representing hours, minutes,
   * and seconds respectively, but they can be over the normal range
   * of numbers associated with 'hours' (00..23), 'minutes' (00..59)
   * and 'seconds' (00..59). The reason for this is that in GTFS, the
   * arrival time of a bus-trip to a stop can be:
   *
   *       27:01:35
   *
   * ie., the next day.
   *
   * @param in_s the string of the lax format 'H:m:s' to convert
   * @return an integer representing the number of seconds
   */

  def convertTimeStrToSeconds(in_s: String): Int =
  {
    val Array(hours, minutes, seconds) = in_s.split(":")

    (hours.toInt * 3600 + minutes.toInt * 60 + seconds.toInt)

  } // method convertTimeStrToSeconds


  /*
   * method: apply
   *
   * @param in_s the string of the lax format 'H:m:s' to convert
   * @return an integer representing the number of seconds
   */

  def apply(in_s: String): Int = convertTimeStrToSeconds(in_s)

} // object ConvertTimeToSeconds

