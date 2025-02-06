package stock.aggregator.model

case class AccountMetric(
  accountId: String,
  value: Double,
  duration: Long,
  metricTimestamp: Long
)
