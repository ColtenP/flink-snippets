package flink.snippets.traffic.light.process;

import flink.snippets.traffic.light.models.TrafficLightPhaseEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class SortEvents extends KeyedProcessFunction<UUID, TrafficLightPhaseEvent, TrafficLightPhaseEvent>  {
  public MapState<Long, TrafficLightPhaseEvent> mapState;

  @Override
  public void open(Configuration parameters) {
    mapState = getRuntimeContext()
        .getMapState(
            new MapStateDescriptor<>(
                "EventMapState",
                Long.class,
                TrafficLightPhaseEvent.class
            )
        );
  }

  @Override
  public void processElement(TrafficLightPhaseEvent event,
                             KeyedProcessFunction<UUID, TrafficLightPhaseEvent, TrafficLightPhaseEvent>.Context ctx,
                             Collector<TrafficLightPhaseEvent> out) throws Exception {
    mapState.put(event.eventTimestamp, event);
    ctx.timerService().registerEventTimeTimer(event.eventTimestamp);
  }

  @Override
  public void onTimer(long timestamp,
                      KeyedProcessFunction<UUID, TrafficLightPhaseEvent, TrafficLightPhaseEvent>.OnTimerContext ctx,
                      Collector<TrafficLightPhaseEvent> out) throws Exception {
    out.collect(mapState.get(timestamp));
    mapState.remove(timestamp);
  }
}
