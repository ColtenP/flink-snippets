package flink.snippets.traffic.light;

import flink.snippets.traffic.light.models.TrafficLightPhaseEvent;
import flink.snippets.traffic.light.models.PhaseChangeMetric;
import flink.snippets.traffic.light.process.Jsonifier;
import flink.snippets.traffic.light.process.PhaseChangeValidator;
import flink.snippets.traffic.light.process.SortEvents;
import flink.snippets.traffic.light.sources.TrafficLightPhaseEventGenerator;
import flink.snippets.traffic.light.window.PhaseChangeMetricReducer;
import flink.snippets.traffic.light.window.PhaseChangeMetricWindowFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class TrafficLightApp {
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

        DataStream<TrafficLightPhaseEvent> unorderedIntersectionEvents = TrafficLightPhaseEventGenerator.toSource(
            env,
            WatermarkStrategy.
                <TrafficLightPhaseEvent>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<TrafficLightPhaseEvent>) (intersectionEvent, timestamp) ->
                        intersectionEvent.eventTimestamp
                )
        );

        DataStream<PhaseChangeMetric> metrics = unorderedIntersectionEvents
            .map(event -> new PhaseChangeMetric(
                event.intersectionId,
                1,
                event.phase == 0 ? 1 : 0
            ))
            .name("IntersectionEvent-to-PhaseChangeMetric")
            .keyBy(event -> event.intersectionId)
            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
            .reduce(new PhaseChangeMetricReducer(), new PhaseChangeMetricWindowFunction());

        metrics
            .keyBy(event -> event.intersectionId)
            .reduce(new PhaseChangeMetricReducer())
            .print("reduced-metrics");

        metrics
            .keyBy(event -> event.intersectionId)
            .window(TumblingEventTimeWindows.of(Time.minutes(15)))
            .reduce(new PhaseChangeMetricReducer(), new PhaseChangeMetricWindowFunction())
            .print("aggregated-metrics");

        env.execute("TrafficLightMetrics");
    }
}
