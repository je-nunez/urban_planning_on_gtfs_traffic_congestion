package src.main.scala.geodecoding

import scala.util.{Failure, Success, Try}

import scala.util.control.NonFatal

import net.liftweb.json._

import src.main.scala.logging.Logging._
import src.main.scala.types.PostalCode
import src.main.scala.cache.KeyValueCache

/**
  * object: GeoDecodingProviderNominatim
  *
  * Implements a GeoDecoding using Nominatim OpenStreetMap API
  * ( http://wiki.openstreetmap.org/wiki/Nominatim )
  *
  * TODO: Local caching to avoid querying the remote Nominatim OpenStreetMap
  */

object GeoDecodingProviderNominatim extends GeoDecodingProvider {

  override protected [this] val urlGeoDecodeFmt =
    "http://nominatim.openstreetmap.org/reverse?lat=%f&lon=%f&format=json&accept-language=en&addressdetails=1"

  override protected [this] val cacheGeoDecode =
    new KeyValueCache[(Double, Double), PostalCode]("GeoDecoderNominatim")

  override protected [this] def parsePostalCodeInAnswer(answerJson: String): Try[PostalCode] = {

    try {
      implicit val formats = net.liftweb.json.DefaultFormats

      val jsonTree = parse(answerJson)
      val address = jsonTree \ "address"
      val postalCode = (address \ "postcode").extract[String]
      val countryCode = ( address \ "country_code").extract[String]
      Success(PostalCode(countryCode, postalCode))
    } catch {
      case NonFatal(e) => {
        logMsg(ERROR, "Error occurred while parsing GeoDecoded JSON: %s".
                        format(e.getMessage))
        Failure(e)
      }
    }

    /*
     * This is an example of the JSON string that Nominatim returns (Oct 2015):
     *
     {
       "place_id":"87278433",

       "licence":"Data OpenStreetMap contributors, ...",
       "osm_type":"way",
       "osm_id":"138141251",
       "lat":"40.7505247",
       "lon":"-73.9935501780078",
       "display_name":"Madison Square Garden, 46, West 31st Street, ... New York City, New York, 10001, United States of America",
       "address":{
                   "stadium":"Madison Square Garden",
                   "house_number":"46",
                   "road":"West 31st Street",
                   "neighbourhood":"Koreatown",
                   "county":"New York County",
                   "city":"New York City",
                   "state":"New York",
                   "postcode":"10001",
                   "country":"United States of America",
                   "country_code":"us"
                 }
     }

     */
  }

}

