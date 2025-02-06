package stock.aggregator.model

case class PricedStockTrade(
  accountId: String,
  stockId: String,
  quantity: Int,
  stockPrice: Option[Double],
  transactionAmount: Option[Double],
  eventTimestamp: Long
)
