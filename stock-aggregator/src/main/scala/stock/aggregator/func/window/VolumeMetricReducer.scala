package stock.aggregator.func.window

import org.apache.flink.api.common.functions.ReduceFunction
import stock.aggregator.model.VolumeMetric

class VolumeMetricReducer extends ReduceFunction[VolumeMetric] {
  def reduce(agg: VolumeMetric, curr: VolumeMetric): VolumeMetric = agg.copy(
    quantity = agg.quantity + curr.quantity
  )
}
