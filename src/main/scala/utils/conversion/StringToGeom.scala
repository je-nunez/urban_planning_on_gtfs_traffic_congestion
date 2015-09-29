package src.main.scala.utils.conversion

import scala.util.control.Exception._
import com.vividsolutions._
import org.geoscript.geometry.io._
import org.geoscript.geometry._

import src.main.scala.logging.Logging._

object StringToGeom {

  /*
   * method: convertSeqCoordsToGeom
   *
   * Converts a string with a sequence of coordinates of the form:
   *
   *      Latit1 Longit1,Latit2 Longit2,...,Latit[n] Longit[n]
   *
   * where each pair "Latit[i] Longit[i]" is separated from the previous
   * and next pairs by a comma ",", in to Geometry.
   *
   * @param inStr the input string with the csv of the coordinates
   * @return the geometry
   */

  def convertSeqCoordsToGeom(coords: Seq[(Double, Double)]):
    jts.geom.Geometry = {

      logMsg(DEBUG, "Converting coordinates " + coords)

      var geometryInstance: jts.geom.Geometry = null

      // see according how many coordinates were given, which GeoScript
      // geometry-builder method should be called
      if (coords.length >= 2) {
        geometryInstance = builder.LineString(coords)
      } else {
        val firstCoord = coords(0)
        geometryInstance = builder.Point(firstCoord._1, firstCoord._2)
      }
      logMsg(DEBUG, "Geometry instance is " + geometryInstance)
      return geometryInstance
    }

  /*
   * method: convertLatitLongitStr
   *
   * Converts a string with a sequence of coordinates of the form:
   *
   *      Latit1 Longit1,Latit2 Longit2,...,Latit[n] Longit[n]
   *
   * where each pair "Latit[i] Longit[i]" is separated from the previous
   * and next pairs by a comma ",", in to Geometry.
   *
   * @param inStr the input string with the csv of the coordinates
   * @return the geometry
   */

  def convertLatitLongitStr(inStr: String): jts.geom.Geometry =
  {

    val coords = catching(classOf[java.lang.RuntimeException]) opt
                   inStr.split("\\s+").map {
                              case pointStr: String => {
                                  val coord = pointStr.split(",")
                                  (coord(0).toDouble, coord(1).toDouble)
                                }
                     }

    if (coords != null && coords.isDefined && coords.get.length > 0) {
      // It is a valid set of coordinates
      val geometryInstance = convertSeqCoordsToGeom(coords.get)
      return geometryInstance
    } else
      return null

  } // method convertLatitLongitStr


  /*
   * method: convertLongitLatitStr
   *
   * Converts a string with a sequence of coordinates of the form:
   *
   *      Longit1 Latit1,Longit2 Latit2,...,Longit[n] Latit[n]
   *
   * where each pair "Longit[i] Latit[i]" is separated from the previous
   * and next pairs by a comma ",", in to Geometry.
   *
   * NOTE: The difference between convertLatitLongitStr(string) and
   *                              convertLongitLatitStr(string)
   *       is which comes first in the string, a latittude or a longitude
   *
   * Eg., the New York City Department of City Planning LION Single-Line
   * Street GeoDB puts "Longitude Latitude" in this order, but the NYC
   * Traffic Speed OpenData puts "Latitude Longitude" in this order.
   *
   * @param inStr the input string with the csv of the coordinates
   * @return the geometry
   */

  def convertLongitLatitStr(inStr: String): jts.geom.Geometry =
  {

    val coords = catching(classOf[java.lang.RuntimeException]) opt
                   inStr.split("\\s+").map {
                              case pointStr: String => {
                                  val coord = pointStr.split(",")
                                  (coord(1).toDouble, coord(0).toDouble)
                              }
                   }

    if (coords != null && coords.isDefined && coords.get.length > 0)  {
      // It is a valid set of coordinates
      val geometryInstance = convertSeqCoordsToGeom(coords.get)
      return geometryInstance
    } else
      return null

  } // method convertLongitLatitStr

  /*
   * method: apply
   *
   * Make the implicit method ("apply(...)") in Scala an alias to
   * convert(s) for this utility object "StringToGeom"
   *
   * @param inStr the input string with the csv of the coordinates.
   * @param isFirstLongitNextLatit whether in the <code>inStr</code> string
   *                              the longitudes come first than the latitudes
   *                              (value of <code>true</code>) or latitudes
   *                              appear first than longitudes (value of
   *                               <code>false</code>).
   * @return the geometry.
   */

  def apply(inStr: String, isFirstLongitNextLatit: Boolean = true):
    jts.geom.Geometry = {

       if (isFirstLongitNextLatit) convertLongitLatitStr(inStr)
                              else convertLatitLongitStr(inStr)
    } // method apply

} // object StringToGeom

