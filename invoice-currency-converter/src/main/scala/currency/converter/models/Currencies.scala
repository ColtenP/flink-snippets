package currency.converter.models

object Currencies {
  val USD = "USD"
  val CAD = "CAD"
  val EUR = "EUR"
  val ILS = "ILS"
  val SEK = "SEK"

  def toList: List[String] = List(USD, CAD, EUR, ILS)
}
