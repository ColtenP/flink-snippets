package flink.snippets.traffic.light;

import flink.snippets.traffic.light.models.IntersectionEvent;
import flink.snippets.traffic.light.models.PhaseChangeMetric;
import flink.snippets.traffic.light.process.Jsonifier;
import flink.snippets.traffic.light.process.PhaseChangeValidator;
import flink.snippets.traffic.light.sources.IntersectionEventGenerator;
import flink.snippets.traffic.light.window.PhaseChangeMetricReducer;
import flink.snippets.traffic.light.window.PhaseChangeMetricWindowFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class IncorrectTrafficLightApp {
    public static void main(String[] args) throws Exception {
        runFlow();
    }

    public static void runFlow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMinutes(1));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        env.getCheckpointConfig().setCheckpointStorage(new Path("file:///tmp"));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<IntersectionEvent> intersectionEvents = IntersectionEventGenerator.toSource(env);

        intersectionEvents
            .keyBy(event -> event.intersectionId)
            .process(new PhaseChangeValidator())
            .name("PhaseChangeValidator")
            .process(new Jsonifier<>())
            .name("PhaseChangeViolation-to-Json")
            .sinkTo(
                FileSink
                    .forRowFormat(
                        new Path("/tmp/phase-change-violations"),
                        new SimpleStringEncoder<String>("utf-8")
                    )
                    .build()
            );

        intersectionEvents
            .map(event -> new PhaseChangeMetric(
                event.intersectionId,
                1,
                event.phase == 0 ? 1 : 0
            ))
            .name("IntersectionEvent-to-PhaseChangeMetric")
            .keyBy(event -> event.intersectionId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .reduce(new PhaseChangeMetricReducer(), new PhaseChangeMetricWindowFunction())
            .name("PhaseChangeMetricWindow")
            .process(new Jsonifier<>())
            .name("PhaseChangeMetric-to-Json")
            .sinkTo(
                FileSink
                    .forRowFormat(
                        new Path("/tmp/phase-change-metrics"),
                        new SimpleStringEncoder<String>("utf-8")
                    )
                    .build()
            );

        env.execute("TrafficLightMetrics");
    }
}
