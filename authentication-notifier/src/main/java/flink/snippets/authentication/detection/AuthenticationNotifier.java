package flink.snippets.authentication.detection;

import flink.snippets.authentication.detection.models.LoginMetadata;
import flink.snippets.authentication.detection.models.LoginNotification;
import flink.snippets.authentication.detection.process.Jsonifier;
import flink.snippets.authentication.detection.process.LoginNotifier;
import flink.snippets.authentication.detection.sources.LoginMetadataGenerator;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.UUID;

public class AuthenticationNotifier {
  public static void main(String[] args) throws Exception {
    runFlow();
  }

  public static void configureCheckpoints(StreamExecutionEnvironment env) {
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointInterval(10000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
    env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMinutes(1));
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
    env.getCheckpointConfig().setCheckpointStorage(new Path("file:///tmp"));
    env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  public static void runFlow() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureCheckpoints(env);

    DataGeneratorSource<LoginMetadata> loginMetadataGenerator = new DataGeneratorSource<>(
        new LoginMetadataGenerator(),
        Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(1_000),
        TypeInformation.of(LoginMetadata.class)
    );

    DataStream<LoginMetadata> loginMetadata = env.fromSource(
        loginMetadataGenerator,
        WatermarkStrategy
            .<LoginMetadata>forBoundedOutOfOrderness(Duration.ofMinutes(1))
            .withTimestampAssigner(
                (SerializableTimestampAssigner<LoginMetadata>) (event, l) -> event.eventTimestamp
            ),
        "LoginMetadata"
    );

    DataStream<LoginNotification> loginNotification = loginMetadata
        .keyBy((KeySelector<LoginMetadata, UUID>) metadata -> metadata.userId)
        .process(new LoginNotifier());

    loginNotification
        .process(new Jsonifier<>())
        .sinkTo(
            FileSink
                .forRowFormat(
                    new Path("/tmp/authentication-notifications"),
                    new SimpleStringEncoder<String>("utf-8")
                )
                .build()
        );

    env.execute("AuthenticationNotifier");
  }
}
