package currency.converter.sources

import currency.converter.models.{Currencies, InvoiceMetadata}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.lang
import java.time.Duration
import java.util.UUID
import scala.util.Random

class InvoiceMetadataGenerator extends GeneratorFunction[java.lang.Long, InvoiceMetadata] {
  @transient private lazy val random = new Random()
  @transient private lazy val currencies = Currencies.toList ++ List("SEK")

  def map(index: lang.Long): InvoiceMetadata = InvoiceMetadata(
    organizationId = new UUID(index % 100, 0),
    id = new UUID(0, index),
    currency = currencies(random.nextInt(currencies.length)),
    amount = Math.round(random.nextDouble() * 1000000) / 100,
    createdAt = System.currentTimeMillis() - Duration.ofDays(7).toMillis,
    settledAt = System.currentTimeMillis()
  )
}

object InvoiceMetadataGenerator {
  def create(env: StreamExecutionEnvironment, rate: Long): DataStream[InvoiceMetadata] =
    env.fromSource(
      new DataGeneratorSource[InvoiceMetadata](
        new InvoiceMetadataGenerator(),
        Long.MaxValue,
        RateLimiterStrategy.perSecond(rate),
        TypeInformation.of(new TypeHint[InvoiceMetadata] {})
      ),
      WatermarkStrategy.noWatermarks(),
      "invoice-metadata"
    )
}

