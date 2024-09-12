package currency.converter.process

import currency.converter.models.{ConvertedInvoiceMetadata, InvoiceMetadataConversionMapping}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class InvoiceCurrencyConverter extends ProcessFunction[InvoiceMetadataConversionMapping, ConvertedInvoiceMetadata] {

  def processElement(source: InvoiceMetadataConversionMapping, ctx: ProcessFunction[InvoiceMetadataConversionMapping, ConvertedInvoiceMetadata]#Context, out: Collector[ConvertedInvoiceMetadata]): Unit = {
    val conversionRatioOption = source.conversions.get(source.currency)

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
}
