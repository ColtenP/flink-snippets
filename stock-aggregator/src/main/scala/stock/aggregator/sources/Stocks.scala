package stock.aggregator.sources

import java.security.MessageDigest

object Stocks {
  lazy val stockIds: List[String] = {
    val md5 = MessageDigest.getInstance("md5")

    // The number of stocks on the NYSE
//    Range(0, 2272)
    Range(0, 148)
      .map { value =>
        md5.digest(value.toString.getBytes("UTF-8"))
          .map("%02x".format(_))
          .mkString("")
          .substring(0, 8)
      }
      .toList
  }
}
