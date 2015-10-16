package src.main.scala.div

// read purpose of this package in the explanation at the text
// file:
//
//     Jane_Jacobs_measures_of_diversity_around_a_latitude_longitud.txt
//
// also in this directory.

abstract class JaneJacobsEntropy {

  def entropyAvailable(latitude: Double, longitude: Double): List[Any]

}

