package flink.snippets.join.sql.sources;

import flink.snippets.join.sql.models.VehicleTelemetry;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

public class VehicleTelemetryGenerator implements GeneratorFunction<Long, VehicleTelemetry> {
  public final Random random = new Random();

  @Override
  public VehicleTelemetry map(Long sequence) {
    random.setSeed(sequence);

    return new VehicleTelemetry(
        new UUID(sequence % 100, 0),
        random.nextDouble() * 50,
        random.nextDouble(),
        random.nextDouble(),
        System.currentTimeMillis()
    );
  }

  public static DataStream<VehicleTelemetry> toSource(StreamExecutionEnvironment env) {
    DataGeneratorSource<VehicleTelemetry> source = new DataGeneratorSource<>(
        new VehicleTelemetryGenerator(),
        Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(20),
        TypeInformation.of(VehicleTelemetry.class)
    );

    return env.fromSource(
            source,
            WatermarkStrategy
                .<VehicleTelemetry>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<VehicleTelemetry>) (vehicleTelemetry, l) -> vehicleTelemetry.telemetryTimestamp),
            "vehicle-telemetry"
        );
  }
}
