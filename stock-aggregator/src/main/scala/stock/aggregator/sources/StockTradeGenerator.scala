package stock.aggregator.sources

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}

import java.lang
import java.security.MessageDigest
import java.time.Duration
import scala.util.Random

class StockTradeGenerator(numberOfAccounts: Long) extends GeneratorFunction[java.lang.Long, StockTrade] {
  @transient private lazy val random = new Random()
  @transient private lazy val md5 = MessageDigest.getInstance("md5")
  @transient private lazy val accountIds = {


    Range(0, numberOfAccounts.toInt)
      .map(md5String)
      .toList
  }

  def map(t: lang.Long): StockTrade = {
    random.setSeed(t)

    val accountId = accountIds((t % numberOfAccounts).toInt)
    val stockId = {
      if (t % 500 == 0) md5String(t)
      else Stocks.stockIds(random.nextInt(Stocks.stockIds.length))
    }
    val quantity = random.nextInt(300) * (if (random.nextDouble() > 0.8) -1 else 1)
    val eventTimestamp = {
      if (t % 50 == 0) System.currentTimeMillis() - Duration.ofMinutes(10).toMillis
      else System.currentTimeMillis() + (
        if (random.nextDouble() < 0.1) -Duration.ofMinutes(2).toMillis
        else 0
      )
    }

    StockTrade(
      accountId = accountId,
      stockId = stockId,
      quantity = quantity,
      eventTimestamp = eventTimestamp
    )
  }

  private def md5String(value: Any): String =
    md5.digest(value.toString.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString("")
      .substring(0, 8)
}

object StockTradeGenerator {
  def create(numberOfAccounts: Long = 500, numberOfEvents: Long = Long.MaxValue, eventsPerSeconds: Int = 500): DataGeneratorSource[StockTrade] =
    new DataGeneratorSource[StockTrade](
      new StockTradeGenerator(numberOfAccounts),
      numberOfEvents,
      RateLimiterStrategy.perSecond(eventsPerSeconds),
      TypeInformation.of(classOf[StockTrade])
    )
}

case class StockTrade(
  accountId: String,
  stockId: String,
  quantity: Int,
  eventTimestamp: Long
)