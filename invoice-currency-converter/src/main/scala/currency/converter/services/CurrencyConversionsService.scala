package currency.converter.services

import currency.converter.models.CurrencyConversion

import scala.util.Random

object CurrencyConversionsService {
  private lazy val random = new Random()

  def get(): List[CurrencyConversion] = List(
    CurrencyConversion(
      currency = "CAD", ratio = 0.74 + (random.nextDouble() / 10)
    ),
    CurrencyConversion(
      currency = "EUR", ratio = 1.10 + (random.nextDouble() / 10)
    ),
    CurrencyConversion(
      currency = "ILS", ratio = 0.26 + (random.nextDouble() / 10)
    )
  )
}
