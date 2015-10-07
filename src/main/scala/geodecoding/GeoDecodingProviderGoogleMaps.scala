package src.main.scala.geodecoding

import scala.util.{Failure, Success, Try}

import scala.util.control.NonFatal

import net.liftweb.json._
import net.liftweb.json.JsonAST.JValue

import src.main.scala.logging.Logging._
import src.main.scala.types.PostalCode
import src.main.scala.cache.KeyValueCache

/**
  * object: GeoDecodingProviderGoogleMaps
  *
  * Implements a GeoDecoding using Google Maps
  *
  * TODO: Local caching to avoid querying the remote Google Maps
  */

object GeoDecodingProviderGoogleMaps extends GeoDecodingProvider {

  override protected [this] val urlGeoDecodeFmt =
    "http://maps.googleapis.com/maps/api/geocode/json?language=en&latlng=%f%%2C%f&sensor=false"

  override protected [this] val cacheGeoDecode =
    new KeyValueCache[(Double, Double), PostalCode]("GeoDecoderGoogleMaps")

  override protected def parsePostalCodeInAnswer(answerJson: String): Try[PostalCode] = {

    try {
      val jsonTree = parse(answerJson)

      implicit val formats = net.liftweb.json.DefaultFormats
      val statusRes = (jsonTree \ "status").extract[String]
      if ( statusRes != "OK")
        return Failure(new Exception("GoogleMaps couldn't geodecode: status %s".
                                       format(statusRes)))

      val firstAddress = (jsonTree \ "results") (0)

      var countryCode: String = ""
      var postalCode: String = ""

      val addressAttributes = (firstAddress \ "address_components").children
      for {
         addressAttribute <- addressAttributes
        } {
            val attrType = (addressAttribute \ "types").extract[Array[String]]

            if (attrType.contains("country")) {
              countryCode = (addressAttribute \ "short_name").extract[String].toLowerCase
            }
            if (attrType.contains("postal_code")) {
              postalCode = (addressAttribute \ "short_name").extract[String].toUpperCase
            }
           } // for-loop

      if (countryCode == "" || postalCode == "")
        Failure(new Exception("GoogleMaps couldn't geodecode latitude given."))
      else
        Success(PostalCode(countryCode, postalCode))

    } catch {
      case NonFatal(e) => {
        logMsg(ERROR, "Error occurred while parsing GeoDecoded JSON: %s".
                        format(e.getMessage))
        Failure(e)
      }
    }

    /*
     * This is an example of the JSON string that GoogleMaps GeoDecode
     * returns (Oct 2015):
     *
     {
     }

     */
  }

}

