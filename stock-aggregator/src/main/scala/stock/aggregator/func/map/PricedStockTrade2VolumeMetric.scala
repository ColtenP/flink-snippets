package stock.aggregator.func.map

import org.apache.flink.api.common.functions.MapFunction
import stock.aggregator.model.{PricedStockTrade, VolumeMetric}

class PricedStockTrade2VolumeMetric extends MapFunction[PricedStockTrade, VolumeMetric] {
  def map(t: PricedStockTrade): VolumeMetric = VolumeMetric(
    stockId = t.stockId,
    quantity = t.quantity,
    duration = 0,
    metricTimestamp = Long.MinValue
  )
}
