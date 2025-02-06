package stock.aggregator

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import stock.aggregator.func.map.{PricedStockTrade2AccountMetric, PricedStockTrade2VolumeMetric}
import stock.aggregator.func.process.StockTradeEnricher
import stock.aggregator.func.window.{AccountMetricReducer, AccountMetricWindowFunction, VolumeMetricReducer, VolumeMetricWindowFunction}
import stock.aggregator.model.{AccountMetric, PricedStockTrade, VolumeMetric}
import stock.aggregator.sink.DummySink
import stock.aggregator.sources._

import java.time.Duration

object StockAggregatorPipeline {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Configure Checkpointing
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(1000)
    env.getCheckpointConfig.setCheckpointTimeout(40)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(Int.MaxValue)

    // Sources
    val stockPrices = env.fromSource(
      StockPriceGenerator.create(eventsPerSeconds = Stocks.stockIds.length),
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofMinutes(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[StockPrice] {
          def extractTimestamp(element: StockPrice, ts: Long): Long = element.eventTimestamp
        }),
      "stock-prices",
      TypeInformation.of(classOf[StockPrice])
    )
    val stockTrades = env.fromSource(
      StockTradeGenerator.create(numberOfAccounts = 23, eventsPerSeconds = 100),
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofMinutes(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[StockTrade] {
          def extractTimestamp(element: StockTrade, ts: Long): Long = element.eventTimestamp
        }),
      "stock-trades",
      TypeInformation.of(classOf[StockTrade])
    )

    // Enrich Stock Price Onto Trades
    val pricedStockTrades = stockTrades
      .keyBy((in: StockTrade) => in.stockId)
      .connect(
        stockPrices.keyBy((in: StockPrice) => in.stockId)
      )
      .process(new StockTradeEnricher)
      .name("stock-trade-enricher")
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofMinutes(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[PricedStockTrade] {
            def extractTimestamp(element: PricedStockTrade, ts: Long): Long = element.eventTimestamp
          })
      )
      .name("priced-stock-trades")

    // Volume Metrics
    val volumeMetrics = pricedStockTrades
      .map(new PricedStockTrade2VolumeMetric)
      .name("unaggregated-volume-metrics")
      .keyBy((in: VolumeMetric) => in.stockId)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(
        new VolumeMetricReducer,
        new VolumeMetricWindowFunction
      )
      .name("volume-metrics")

    // Account Metrics
    val accountMetrics = pricedStockTrades
      .map(new PricedStockTrade2AccountMetric)
      .name("unaggregated-account-metrics")
      .keyBy((in: AccountMetric) => in.accountId)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(
        new AccountMetricReducer,
        new AccountMetricWindowFunction
      )
      .name("account-metrics")

    volumeMetrics.addSink(new DummySink).name("volume-metrics")
    accountMetrics.addSink(new DummySink).name("account-metrics")

    env.execute()
  }
}
