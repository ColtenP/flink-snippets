package flink.snippets.traffic.light.process;

import flink.snippets.traffic.light.models.IntersectionEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EventTimeScrambler extends KeyedProcessFunction<UUID, IntersectionEvent, IntersectionEvent> {

  public Random random;
  public MapState<Long, IntersectionEvent> mapState;

  @Override
  public void open(Configuration parameters) {
    random = new Random();
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
    long triggerTimestamp;
    do {
      triggerTimestamp = event.eventTimestamp - ((long) (random.nextDouble() * Duration.ofMinutes(5).toMillis()));
    } while (mapState.contains(triggerTimestamp));

    mapState.put(triggerTimestamp, event);
    ctx.timerService().registerEventTimeTimer(triggerTimestamp);
  }

  @Override
  public void onTimer(long timestamp,
                      KeyedProcessFunction<UUID, IntersectionEvent, IntersectionEvent>.OnTimerContext ctx,
                      Collector<IntersectionEvent> out) throws Exception {
    List<Map.Entry<Long, IntersectionEvent>> outEvents = StreamSupport
        .stream(mapState.entries().spliterator(), false)
        .filter(entry -> entry.getKey() <= timestamp)
        .collect(Collectors.toList());

    for (Map.Entry<Long, IntersectionEvent> entry : outEvents) {
      out.collect(entry.getValue());
      mapState.remove(entry.getKey());
    }
  }
}
