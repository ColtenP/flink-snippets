package currency.converter.process

import currency.converter.models.{ConvertedInvoiceMetadata, CurrencyConversion, InvoiceMetadata}
import currency.converter.process.InvoiceMetadataConverter.CURRENCY_CONVERSION_STATE_DESCRIPTOR
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import java.util.UUID

class InvoiceMetadataConverter extends KeyedBroadcastProcessFunction[UUID, InvoiceMetadata, CurrencyConversion, ConvertedInvoiceMetadata] {

  def processElement(
                      source: InvoiceMetadata,
                      ctx: KeyedBroadcastProcessFunction[UUID, InvoiceMetadata, CurrencyConversion, ConvertedInvoiceMetadata]#ReadOnlyContext,
                      out: Collector[ConvertedInvoiceMetadata]): Unit = {
    val conversionRatioOption = Option(ctx
      .getBroadcastState(CURRENCY_CONVERSION_STATE_DESCRIPTOR)
      .get(source.currency)
    )

    // If the conversion ratio is not in the broadcast state, it's USD (because our mapping is totally full and not
    // incomplete)
    val conversionRatio = conversionRatioOption.getOrElse(1.0)

    out.collect(ConvertedInvoiceMetadata(
      organizationId = source.organizationId,
      id = source.id,
      sourceCurrency = source.currency,
      sourceAmount = source.amount,
      convertedCurrency = "USD",
      convertedAmount = source.amount * conversionRatio,
      createdAt = source.createdAt,
      settledAt = source.settledAt
    ))
  }

  def processBroadcastElement(
                               value: CurrencyConversion,
                               ctx: KeyedBroadcastProcessFunction[UUID, InvoiceMetadata, CurrencyConversion, ConvertedInvoiceMetadata]#Context,
                               out: Collector[ConvertedInvoiceMetadata]
                             ): Unit = {
    ctx.getBroadcastState(CURRENCY_CONVERSION_STATE_DESCRIPTOR).put(value.currency, value.ratio)
  }
}

object InvoiceMetadataConverter {
  val CURRENCY_CONVERSION_STATE_DESCRIPTOR =
    new MapStateDescriptor[String, Double]("CurrencyConversionState", classOf[String], classOf[Double])
}