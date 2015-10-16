package src.main.scala.div

import scala.collection.mutable.ListBuffer

import net.liftweb.json._
import net.liftweb.json.JsonAST.JValue

import src.main.scala.logging.Logging._

class Uber extends JaneJacobsEntropy {

  // Use Uber's /estimates/time because it gives Uber's ETAs for
  // ___several offers___ (in seconds), so the ___smaller___ the ETAs
  // and the ___higher the number of offers___ given by Uber for this
  // location (latitude, longitude), then we might say that this
  // location (latitude, longitude) has a higher Jane Jacobs' enthropy
  // (at that time of the day when the query to Uber is done)
  //
  // Note:
  // The use of Uber is not related to the General Transit Feed Specification,
  // but specifically only to Jane Jacobs' enthropy of diversity (see her
  // ideas in her book 'The Life and Death of Great American Cities').
  // GTFS is more related to traffic and more concrete Urban Planning.

  override def entropyAvailable(latitude: Double, longitude: Double): List[Any] = {
    // call Uber's /estimates/time for (latitude, longitude)

    val resultingEntropy = new ListBuffer[Double]()

    val uberAvailServicesReq =
      "https://sandbox-api.uber.com/v1/sandbox/estimates/time?start_latitude=%f&start_longitude=%f"

    try {
      val uberServicesReq = uberAvailServicesReq.format(latitude, longitude)
      // TODO: needs OAuth
      val src = scala.io.Source.fromURL(uberServicesReq)
      val uberAvailableServices = src.mkString
      val jsonUberAvailableServices = parse(uberAvailableServices)

      implicit val formats = net.liftweb.json.DefaultFormats
      val timesAvailServices = (jsonUberAvailableServices \ "times").children
      for {
         timeAvailService <- timesAvailServices
        } {
            // Get the ETA when it can be available, the smaller the value
            // the better, since it can be available the sooner according
            // to Uber, so this place has greater Jane Jacobs's entropy
            val timeETAService = (timeAvailService \ "estimate").extract[Double]
            val janeJacobsEntropy = 1000000.0 - timeETAService // closer to 1M: better

            resultingEntropy += janeJacobsEntropy
        }
    } catch {
          case e: java.io.IOException => {
                     logMsg(ERROR, "I/O error occurred " + e.getMessage)
            }
    }

    // One issue here is how to interpret the results among several providers of
    // Jane Jacobs entropy for a coordinate, ie., same for Uber, Picasa's likes,
    // Crimes data (like http://maps.nyc.gov/crime/,
    // ftp://webftp.vancouver.ca/opendata/csv/crime_2015.csv), and other data
    // sources for Jane Jacobs entropy for Urban City Planning
    // (eg., http://www.nyc.gov/html/dcp/html/about/pr121306.shtml)

    return resultingEntropy.toList
  }

}

