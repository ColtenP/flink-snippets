package stock.aggregator.func.window

import org.apache.flink.api.common.functions.ReduceFunction
import stock.aggregator.model.AccountMetric

class AccountMetricReducer extends ReduceFunction[AccountMetric] {
  def reduce(agg: AccountMetric, curr: AccountMetric): AccountMetric = agg.copy(
    value = agg.value + curr.value
  )
}
