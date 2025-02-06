package stock.aggregator.func.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector
import stock.aggregator.model.VolumeMetric

import java.lang

class VolumeMetricWindowFunction extends WindowFunction[VolumeMetric, VolumeMetric, String, TimeWindow] {
  def apply(key: String, w: TimeWindow, iterable: lang.Iterable[VolumeMetric], collector: Collector[VolumeMetric]): Unit =
    iterable.forEach(event =>
      collector.collect(event.copy(
        duration = w.getEnd - w.getStart,
        metricTimestamp = w.getStart
      ))
    )
}
