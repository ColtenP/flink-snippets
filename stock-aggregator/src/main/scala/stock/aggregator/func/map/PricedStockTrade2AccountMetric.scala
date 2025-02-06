package stock.aggregator.func.map

import org.apache.flink.api.common.functions.MapFunction
import stock.aggregator.model.{AccountMetric, PricedStockTrade}

class PricedStockTrade2AccountMetric extends MapFunction[PricedStockTrade, AccountMetric] {
  def map(t: PricedStockTrade): AccountMetric = AccountMetric(
    accountId = t.accountId,
    value = t.stockPrice.getOrElse(0.0),
    duration = 0,
    metricTimestamp = Long.MinValue
  )
}
