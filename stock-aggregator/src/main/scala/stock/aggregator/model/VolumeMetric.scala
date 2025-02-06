package stock.aggregator.model

case class VolumeMetric(
  stockId: String,
  quantity: Integer,
  duration: Long,
  metricTimestamp: Long
)
