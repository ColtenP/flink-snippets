package flink.snippets.join.sql.sources;

import flink.snippets.join.sql.models.VehicleEvent;
import flink.snippets.join.sql.models.VehicleEventType;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.UUID;

public class VehicleEventGenerator implements GeneratorFunction<Long, VehicleEvent> {

  @Override
  public VehicleEvent map(Long sequence) {
    return new VehicleEvent(
        new UUID(sequence % 100, 0),
        VehicleEventType.DRIVING,
        System.currentTimeMillis()
    );
  }

  public static DataStream<VehicleEvent> toSource(StreamExecutionEnvironment env) {
    DataGeneratorSource<VehicleEvent> source = new DataGeneratorSource<>(
        new VehicleEventGenerator(),
        Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(20),
        TypeInformation.of(VehicleEvent.class)
    );

    return env.fromSource(
            source,
            WatermarkStrategy
                .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<VehicleEvent>) (vehicleEvent, l) -> vehicleEvent.eventTimestamp),
            "vehicle-events"
        );
  }
}
