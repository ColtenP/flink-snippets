from pyflink.common import WatermarkStrategy, Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows

# create a batch stream table environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

# create message input data
messages = env.from_collection(
    [
        (1, 'Hi', 'Colten', 1716378540000),
        (2, 'Hello', 'Alex', 1716378541000),
        (3, 'How are you?', 'Colten', 1716378542000),
        (4, 'Good, what is your name?', 'Alex', 1716378544000),
        (5, 'Colten, who are you?', 'Colten', 1716378546000),
        (6, 'Alex, nice to meet you!', 'Alex', 1716378548000),
        (7, 'Likewise.', 'Colten', 1716378549000)
    ]
)


# watermark data
class UserMessageTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, message, record_timestamp: int) -> int:
        return message[3]


watermarked_messages = (
    messages
    .assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(UserMessageTimestampAssigner())
    )
)


# window into metrics
message_metrics = (
    watermarked_messages
    .map(lambda message: (message[2], 1))
    .key_by(lambda message: message[0])
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(lambda a, b: (a[0], a[1] + b[1]))
)

message_metrics.print()
env.execute('UserMessageMetrics')
