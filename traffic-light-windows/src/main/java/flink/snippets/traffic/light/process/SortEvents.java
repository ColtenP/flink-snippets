package flink.snippets.traffic.light.process;

import flink.snippets.traffic.light.models.IntersectionEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class SortEvents extends KeyedProcessFunction<UUID, IntersectionEvent, IntersectionEvent>  {
  public MapState<Long, IntersectionEvent> mapState;

  @Override
  public void open(Configuration parameters) {
    mapState = getRuntimeContext()
        .getMapState(
            new MapStateDescriptor<>(
                "EventMapState",
                Long.class,
                IntersectionEvent.class
            )
        );
  }

  @Override
  public void processElement(IntersectionEvent event,
                             KeyedProcessFunction<UUID, IntersectionEvent, IntersectionEvent>.Context ctx,
                             Collector<IntersectionEvent> out) throws Exception {
    mapState.put(event.eventTimestamp, event);
    ctx.timerService().registerEventTimeTimer(event.eventTimestamp);
  }

  @Override
  public void onTimer(long timestamp,
                      KeyedProcessFunction<UUID, IntersectionEvent, IntersectionEvent>.OnTimerContext ctx,
                      Collector<IntersectionEvent> out) throws Exception {
    out.collect(mapState.get(timestamp));
    mapState.remove(timestamp);
  }
}
