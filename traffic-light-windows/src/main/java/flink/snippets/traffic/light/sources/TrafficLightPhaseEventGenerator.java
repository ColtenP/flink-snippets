package flink.snippets.traffic.light.sources;

import flink.snippets.traffic.light.models.TrafficLightPhaseEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * Generates an out-of-order stream of IntersectionEvents
 */
public class TrafficLightPhaseEventGenerator implements GeneratorFunction<Long, TrafficLightPhaseEvent> {
  public final List<TrafficLightPhaseEvent> queuedEvents = new ArrayList<>();
  public final Random random = new Random();
  public int lastPickedIndex = 5;
  public final UUID intersectionId = UUID.randomUUID();
  public Tuple2<Integer, Integer> latestPhases = Tuple2.of(1, 0);
  public Long lastTimestamp = System.currentTimeMillis();

  @Override
  public void open(SourceReaderContext readerContext) {
    IntStream
        .range(0, 20)
        .forEach((i) -> generateEvent());
  }

  @Override
  public TrafficLightPhaseEvent map(Long sequence) {
    lastPickedIndex = (lastPickedIndex + 5) % 10;
    TrafficLightPhaseEvent event = queuedEvents.get(lastPickedIndex);
    queuedEvents.remove(lastPickedIndex);
    generateEvent();

    return event;
  }

  public void generateEvent() {
    int lastPhase = latestPhases.f1;
    boolean isEmergencyVehicleComing = lastPhase != 0 && random.nextDouble() < 0.05;
    int nextPhase = isEmergencyVehicleComing
        ? 0
        : (lastPhase % 8) + 1;

    latestPhases = Tuple2.of(lastPhase, nextPhase);
    lastTimestamp = lastTimestamp + Duration.ofSeconds(25).toMillis();

    queuedEvents.add(new TrafficLightPhaseEvent(
        intersectionId,
        UUID.randomUUID(),
        lastTimestamp,
        nextPhase
    ));
  }

  public static DataStream<TrafficLightPhaseEvent> toSource(StreamExecutionEnvironment env, WatermarkStrategy<TrafficLightPhaseEvent> watermarkStrategy, long numberOfEvents, double recordPerSecond) {
    DataGeneratorSource<TrafficLightPhaseEvent> source = new DataGeneratorSource<>(
        new TrafficLightPhaseEventGenerator(),
        numberOfEvents,
        RateLimiterStrategy.perSecond(recordPerSecond),
        TypeInformation.of(TrafficLightPhaseEvent.class)
    );

    return env.fromSource(
            source,
            watermarkStrategy,
            "IntersectionEventGenerator"
        );
  }

  public static DataStream<TrafficLightPhaseEvent> toSource(StreamExecutionEnvironment env, WatermarkStrategy<TrafficLightPhaseEvent> watermarkStrategy) {
    return toSource(env, watermarkStrategy, 100_000, 25);
  }
}
