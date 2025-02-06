package stock.aggregator.sources

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}

import java.lang
import scala.collection.mutable
import scala.util.Random

class StockPriceGenerator extends GeneratorFunction[java.lang.Long, StockPrice] {
  @transient private lazy val random = new Random()
  @transient private lazy val previousPrices: mutable.Map[String, Double] = mutable.Map()

  def map(t: lang.Long): StockPrice = {
    random.setSeed(t)

    val stockId = Stocks.stockIds(random.nextInt(Stocks.stockIds.length))
    val previousPrice = {
      if (previousPrices.contains(stockId)) previousPrices(stockId)
      else Math.round(random.nextDouble() * random.nextDouble() * 50.0 * 100.0) / 100.0
    }
    val currentPrice = {
      val rawPrice = random.nextDouble() match {
        case value if value < 0.5 => previousPrice + (random.nextDouble() / 5.0)
        case value if value < 0.8 => Math.max(previousPrice - (random.nextDouble() / 5.0), 0)
        case value if value < 0.9 => Math.max(previousPrice - (random.nextDouble() * 5.0), 0)
        case _ => previousPrice + (random.nextDouble() * 5.0)
      }

      Math.round(rawPrice * 100.0) / 100.0
    }

    previousPrices += stockId -> currentPrice

    StockPrice(
      stockId = stockId,
      price = currentPrice,
      eventTimestamp = System.currentTimeMillis()
    )
  }
}

object StockPriceGenerator {
  def create(numberOfEvents: Long = Long.MaxValue, eventsPerSeconds: Int = 500): DataGeneratorSource[StockPrice] =
    new DataGeneratorSource[StockPrice](
      new StockPriceGenerator,
      numberOfEvents,
      RateLimiterStrategy.perSecond(eventsPerSeconds),
      TypeInformation.of(classOf[StockPrice])
    )
}

case class StockPrice(
  stockId: String,
  price: Double,
  eventTimestamp: Long
)