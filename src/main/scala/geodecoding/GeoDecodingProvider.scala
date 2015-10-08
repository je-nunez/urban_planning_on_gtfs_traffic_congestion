package src.main.scala.geodecoding

import scala.util.{Failure, Success, Try}
import scala.io.Source

import src.main.scala.logging.Logging._
import src.main.scala.types.PostalCode
import src.main.scala.cache.KeyValueCache


abstract class GeoDecodingProvider extends Object {

  // There can be many functions in this trait: it is just defined the first,
  // to get the postal-code associated with a geographical coordinate

  protected [this] val urlGeoDecodeFmt: String

  protected [this] val cacheGeoDecode: KeyValueCache[(Double, Double), PostalCode]

  def convertLatLongToPostalCode(latitude: Double, longitude: Double): Try[PostalCode] = {

    // check whether this (latitude/longitude) is already in the cache for this Geodecoder
    val cacheKey = (latitude, longitude)
    val cachedPostalCode: Option[PostalCode] = cacheGeoDecode.get(cacheKey)
    cachedPostalCode match {
      case Some(postalCode) => {
         logMsg(DEBUG, "Geodecoding for latitude,longitude=(%f, %f) already cached".
                          format(latitude, longitude))
         return Success(postalCode)
      }
      case None => {
         logMsg(DEBUG, "Geodecoding for latitude,longitude=(%f, %f) has not been cached before".
                          format(latitude, longitude))
      }
    }

    // this (latitude/longitude) is not in the cache for this Geodecoder
    val urlGeoDecode = urlGeoDecodeFmt.format(latitude, longitude)
    try {
      // val aCmdLineArg = cache(i.toString)
      val src = Source.fromURL(urlGeoDecode)
      val geoDecodeResult = src.mkString
      val result = parsePostalCodeInAnswer(geoDecodeResult)
      if (result.isSuccess) {
        // The (latitude/longitude) were finally geodecoded in parsePostalCodeInAnswer
        logMsg(DEBUG, "Caching the geodecoding for latitude,longitude=(%f, %f)".
                         format(latitude, longitude))
        cacheGeoDecode.add(cacheKey, result.get)  // .get returns a PostalCode
      }
      result
      // case (value, index) => cache.add(index.toString, value)
    } catch {
      case e: java.io.IOException => {
                   logMsg(ERROR, "I/O error occurred while GeoDecoding: %s".
                                   format(e.getMessage))
                   Failure(e)
      }
    } // end of catch
    
  } // end of "def convertLatLongToPostalCode"


  protected [this] def parsePostalCodeInAnswer(answerJson: String): Try[PostalCode]


  def apply(latitude: Double, longitude: Double): Try[PostalCode] =
    convertLatLongToPostalCode(latitude, longitude)

}

