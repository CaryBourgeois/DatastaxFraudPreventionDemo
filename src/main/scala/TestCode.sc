

import scala.collection.immutable.TreeMap
import scala.util.Random

class Location {

  val r = Random

  var locationList = TreeMap[Double, Tuple2[String, String]](
          0.25 -> ("DZ", ""),
          0.5 -> ("UG", ""),
          0.75 -> ("SD", ""),
          1.0 -> ("US", "CA")
  )

  def nextLocation() : Tuple2[String, String] = {
    val testVal : Double = r.nextDouble()

    for ((key, value) <- locationList) { if (key >= testVal) { return value } }

    return ("All Other", "")
  }
}

val loc = new Location
val newLoc = loc.nextLocation()
newLoc._1
newLoc._2
