package stock.aggregator.func.process

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import org.slf4j.LoggerFactory
import stock.aggregator.func.process.StockTradeEnricher.{ERROR_OUTPUT, STOCK_PRICE_STATE_DESCRIPTOR}
import stock.aggregator.model.PricedStockTrade
import stock.aggregator.sources.{StockPrice, StockTrade}

import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.Random

class StockTradeEnricher extends KeyedCoProcessFunction[String, StockTrade, StockPrice, PricedStockTrade] {
  private lazy val log = LoggerFactory.getLogger(classOf[StockTradeEnricher])
  @transient var priceState: MapState[Long, Double] = _
  @transient lazy val random = new Random()

  override def open(parameters: Configuration): Unit = {
    priceState = getRuntimeContext.getMapState(STOCK_PRICE_STATE_DESCRIPTOR)
  }

  def processElement1(
                       trade: StockTrade,
                       ctx: KeyedCoProcessFunction[String, StockTrade, StockPrice, PricedStockTrade]#Context,
                       out: Collector[PricedStockTrade]
                     ): Unit = {
    getPriceAtTimestamp(trade.eventTimestamp) match {
      case Some(price) =>
        out.collect(PricedStockTrade(
          accountId = trade.accountId,
          stockId = trade.stockId,
          quantity = trade.quantity,
          stockPrice = Some(price),
          transactionAmount = Some(price * trade.quantity),
          eventTimestamp = trade.eventTimestamp
        ))
      case None =>
        log.error(s"There was no price data for stock with id ${trade.stockId}")
        ctx.output(ERROR_OUTPUT, PricedStockTrade(
          accountId = trade.accountId,
          stockId = trade.stockId,
          quantity = trade.quantity,
          stockPrice = None,
          transactionAmount = None,
          eventTimestamp = trade.eventTimestamp
        ))
    }
  }

  def processElement2(
                       price: StockPrice,
                       ctx: KeyedCoProcessFunction[String, StockTrade, StockPrice, PricedStockTrade]#Context,
                       out: Collector[PricedStockTrade]
                     ): Unit = {
    priceState.put(price.eventTimestamp, price.price)
    ctx.timerService().registerEventTimeTimer(price.eventTimestamp + Duration.ofMinutes(3).toMillis)
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedCoProcessFunction[String, StockTrade, StockPrice, PricedStockTrade]#OnTimerContext,
                        out: Collector[PricedStockTrade]): Unit = {
    priceState.remove(timestamp - Duration.ofMinutes(3).toMillis)
  }

  def getPriceAtTimestamp(timestamp: Long): Option[Double] = {
    val ts = priceState.keys().asScala
      .toSeq
      .sorted
      .reverse
      .find(_ < timestamp)

    if (ts.isDefined) Option(priceState.get(ts.get))
    else None
  }
}

object StockTradeEnricher {
  val STOCK_PRICE_STATE_DESCRIPTOR = new MapStateDescriptor[Long, Double](
    "StockPrice",
    classOf[Long],
    classOf[Double]
  )

  val ERROR_OUTPUT = new OutputTag[PricedStockTrade]("Error", TypeInformation.of(classOf[PricedStockTrade]))
}


