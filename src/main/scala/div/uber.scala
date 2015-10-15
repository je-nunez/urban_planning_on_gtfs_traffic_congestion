package src.main.scala.div

class Uber extends JaneJacobsEnthropy {

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

  def enthropyAvailable(latitude: Double, longitude: Double): List[Any] = {
    // call Uber's /estimates/time for (latitude, longitude)
  }

}

