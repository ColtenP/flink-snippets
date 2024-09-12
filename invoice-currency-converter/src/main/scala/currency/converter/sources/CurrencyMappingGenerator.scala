package currency.converter.sources

import currency.converter.models.CurrencyConversion
import currency.converter.services.CurrencyConversionsService
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.lang

class CurrencyMappingGenerator extends GeneratorFunction[java.lang.Long, List[CurrencyConversion]] {
  def map(t: lang.Long): List[CurrencyConversion] = CurrencyConversionsService.get()
}

object CurrencyMappingGenerator {
  def create(env: StreamExecutionEnvironment): DataStream[List[CurrencyConversion]] =
    env.fromSource(
      new DataGeneratorSource[List[CurrencyConversion]](
        new CurrencyMappingGenerator(),
        Long.MaxValue,
        RateLimiterStrategy.perCheckpoint(1),
        TypeInformation.of(new TypeHint[List[CurrencyConversion]] {})
      ),
      WatermarkStrategy.noWatermarks(),
      "currency-mappings"
    )
}

