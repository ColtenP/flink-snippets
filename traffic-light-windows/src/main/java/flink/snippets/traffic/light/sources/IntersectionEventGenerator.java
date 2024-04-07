package flink.snippets.traffic.light.sources;

import flink.snippets.traffic.light.models.IntersectionEvent;
import flink.snippets.traffic.light.process.EventTimeScrambler;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

public class IntersectionEventGenerator implements GeneratorFunction<Long, IntersectionEvent> {
  public final Random random = new Random();
  public final UUID intersectionId = UUID.randomUUID();
  public Tuple2<Integer, Integer> latestPhases = Tuple2.of(1, 0);
  public Long lastTimestamp = System.currentTimeMillis();

  @Override
  public IntersectionEvent map(Long sequence) {
    int lastPhase = latestPhases.f1;
    boolean isEmergencyVehicleComing = lastPhase != 0 && random.nextDouble() < 0.05;
    int nextPhase = isEmergencyVehicleComing
        ? 0
        : (lastPhase % 8) + 1;

    latestPhases = Tuple2.of(lastPhase, nextPhase);
    lastTimestamp = lastTimestamp + Duration.ofSeconds(25).toMillis();

    return new IntersectionEvent(
        intersectionId,
        UUID.randomUUID(),
        lastTimestamp,
        nextPhase
    );
  }

  public static DataStream<IntersectionEvent> toSource(StreamExecutionEnvironment env) {
    DataGeneratorSource<IntersectionEvent> source = new DataGeneratorSource<>(
        new IntersectionEventGenerator(),
        1_000_000_000,
        RateLimiterStrategy.perSecond(25),
        TypeInformation.of(IntersectionEvent.class)
    );

    return env.fromSource(
            source,
            WatermarkStrategy.
                <IntersectionEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<IntersectionEvent>) (intersectionEvent, timestamp) ->
                        intersectionEvent.eventTimestamp
                ),
            "IntersectionEventGenerator"
        )
        .keyBy(event -> event.intersectionId)
        .process(new EventTimeScrambler())
        .name("EventTimeScrambler");
  }
}
