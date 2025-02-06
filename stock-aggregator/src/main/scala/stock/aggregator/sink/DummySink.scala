package stock.aggregator.sink

import org.apache.flink.streaming.api.functions.sink.SinkFunction

class DummySink[T] extends SinkFunction[T]
