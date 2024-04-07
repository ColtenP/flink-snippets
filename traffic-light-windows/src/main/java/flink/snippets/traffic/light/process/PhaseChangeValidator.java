package flink.snippets.traffic.light.process;

import flink.snippets.traffic.light.models.IntersectionEvent;
import flink.snippets.traffic.light.models.PhaseChangeViolation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class PhaseChangeValidator extends KeyedProcessFunction<UUID, IntersectionEvent, PhaseChangeViolation> {

  public ValueState<IntersectionEvent> lastSeenEventState;

  @Override
  public void open(Configuration parameters) {
    lastSeenEventState = getRuntimeContext()
        .getState(
            new ValueStateDescriptor<>(
                "LastSeenEvent",
                IntersectionEvent.class
            )
        );
  }

  @Override
  public void processElement(IntersectionEvent event,
                             KeyedProcessFunction<UUID, IntersectionEvent, PhaseChangeViolation>.Context ctx,
                             Collector<PhaseChangeViolation> out) throws Exception {
    IntersectionEvent lastSeenEvent = lastSeenEventState.value();
    if (lastSeenEvent == null) {
      lastSeenEventState.update(event);
      return;
    } else {
      lastSeenEventState.update(event);
    }

    if (event.phase < 0 || event.phase > 8) {
      out.collect(new PhaseChangeViolation(lastSeenEvent, event));
    } else if (lastSeenEvent.phase != 0 && event.phase != 0 && event.phase - (lastSeenEvent.phase % 8) > 1) {
      out.collect(new PhaseChangeViolation(lastSeenEvent, event));
    }
  }
}
