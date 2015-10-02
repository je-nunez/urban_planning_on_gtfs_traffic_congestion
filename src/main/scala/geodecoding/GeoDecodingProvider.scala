package src.main.scala.geodecoding

import scala.util.{Failure, Success, Try}

import src.main.scala.types.PostalCode


trait GeoDecodingProvider {

  // There can be many functions in this trait: it is just defined the first,
  // to get the postal-code associated with a geographical coordinate

  def convertLatLongToPostalCode(Latitude: Double, Longitude: Double):
    Try[PostalCode]

}

