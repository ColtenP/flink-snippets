package flink.snippets.traffic.light.window;

import flink.snippets.traffic.light.models.PhaseChangeMetric;
import org.apache.flink.api.common.functions.ReduceFunction;

public class PhaseChangeMetricReducer implements ReduceFunction<PhaseChangeMetric> {
  @Override
  public PhaseChangeMetric reduce(PhaseChangeMetric accumulator, PhaseChangeMetric metric) {
    return new PhaseChangeMetric(
        accumulator.intersectionId,
        accumulator.phases + metric.phases,
        accumulator.emergencyPhases + metric.emergencyPhases
    );
  }
}
