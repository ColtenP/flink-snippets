package stock.aggregator.func.window

import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import stock.aggregator.model.AccountMetric

import java.lang

class AccountMetricWindowFunction extends WindowFunction[AccountMetric, AccountMetric, String, TimeWindow] {
  def apply(key: String, w: TimeWindow, iterable: lang.Iterable[AccountMetric], collector: Collector[AccountMetric]): Unit =
    iterable.forEach(event =>
      collector.collect(event.copy(
        duration = w.getEnd - w.getStart,
        metricTimestamp = w.getStart
      ))
    )
}
