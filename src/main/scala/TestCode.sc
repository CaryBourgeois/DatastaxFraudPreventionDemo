val r = scala.util.Random
def createItems (numItems: Int) : (String, Double) = {
  var total = 0.0
  val message = for (item <- 1 to numItems) yield {
    val itemVal = r.nextDouble()*1000
    total = total + itemVal
    s"Item-${item} : " + "%1.2f".format(itemVal)
  }
  return (s"{${message.mkString(", ")}}", total)
}
createItems(3)

"%016d".format(2011)

val testStr = "Item-50037->473.76, Item-90394->891.27"

val testMap = testStr.split(",").map(_.split("->")).map { case Array(k,v) => (k, v.toDouble)}.toMap

val payload = "CHECK"

val status = if (!payload.equalsIgnoreCase("check")) {
  payload
} else if (r.nextGaussian().abs > 0.01) {
  "APPROVED"
} else {
  "DECLINED"
}
