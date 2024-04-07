package flink.snippets.traffic.light.window;

import flink.snippets.traffic.light.models.PhaseChangeMetric;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class PhaseChangeMetricWindowFunction extends ProcessWindowFunction<PhaseChangeMetric, PhaseChangeMetric, UUID, TimeWindow> {
  @Override
  public void process(UUID uuid,
                      ProcessWindowFunction<PhaseChangeMetric, PhaseChangeMetric, UUID, TimeWindow>.Context context,
                      Iterable<PhaseChangeMetric> metrics,
                      Collector<PhaseChangeMetric> out) {
    for (PhaseChangeMetric metric : metrics) {
      out.collect(new PhaseChangeMetric(
          metric.intersectionId,
          context.window().getStart(),
          context.window().getEnd(),
          metric.phases,
          metric.emergencyPhases
      ));
    }
  }
}
