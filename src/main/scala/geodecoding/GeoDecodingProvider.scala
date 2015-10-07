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

  def convertLatLongToPostalCode(Latitude: Double, Longitude: Double): Try[PostalCode] = {

      val urlGeoDecode = urlGeoDecodeFmt.format(Latitude, Longitude)
      try {
        // val aCmdLineArg = cache(i.toString)
        val src = Source.fromURL(urlGeoDecode)
        val geoDecodeResult = src.mkString
        parsePostalCodeInAnswer(geoDecodeResult)
        // case (value, index) => cache.add(index.toString, value)
      } catch {
        case e: java.io.IOException => {
                     logMsg(ERROR, "I/O error occurred while GeoDecoding: %s".
                                     format(e.getMessage))
                     Failure(e)
          }
      }
  }

  protected [this] def parsePostalCodeInAnswer(answerJson: String): Try[PostalCode]

  def apply(Latitude: Double, Longitude: Double): Try[PostalCode] =
    convertLatLongToPostalCode(Latitude, Longitude)

}

