package currency.converter.process

import currency.converter.models.{CurrencyConversion, InvoiceMetadata, InvoiceMetadataConversionMapping}
import currency.converter.process.InvoiceMetadataEnricher.CURRENCY_CONVERSION_STATE_DESCRIPTOR
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import java.util.UUID
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class InvoiceMetadataEnricher extends KeyedBroadcastProcessFunction[UUID, InvoiceMetadata, CurrencyConversion, InvoiceMetadataConversionMapping] {

  def processElement(
                      source: InvoiceMetadata,
                      ctx: KeyedBroadcastProcessFunction[UUID, InvoiceMetadata, CurrencyConversion, InvoiceMetadataConversionMapping]#ReadOnlyContext,
                      out: Collector[InvoiceMetadataConversionMapping]): Unit = {
    val conversions = ctx.getBroadcastState(CURRENCY_CONVERSION_STATE_DESCRIPTOR)
      .immutableEntries()
      .asScala
      .map((entry: java.util.Map.Entry[String, Double]) => entry.getKey -> entry.getValue)
      .toMap

    out.collect(InvoiceMetadataConversionMapping(
      organizationId = source.organizationId,
      id = source.id,
      currency = source.currency,
      amount = source.amount,
      createdAt = source.createdAt,
      settledAt = source.settledAt,
      conversions = conversions
    ))
  }

  def processBroadcastElement(
                               value: CurrencyConversion,
                               ctx: KeyedBroadcastProcessFunction[UUID, InvoiceMetadata, CurrencyConversion, InvoiceMetadataConversionMapping]#Context,
                               out: Collector[InvoiceMetadataConversionMapping]
                             ): Unit = {
    ctx.getBroadcastState(CURRENCY_CONVERSION_STATE_DESCRIPTOR).put(value.currency, value.ratio)
  }
}

object InvoiceMetadataEnricher {
  val CURRENCY_CONVERSION_STATE_DESCRIPTOR =
    new MapStateDescriptor[String, Double]("CurrencyConversionState", classOf[String], classOf[Double])
}