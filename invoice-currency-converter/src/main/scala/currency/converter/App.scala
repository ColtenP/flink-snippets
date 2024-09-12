package currency.converter

import currency.converter.models.{CurrencyConversion, InvoiceMetadata}
import currency.converter.process.InvoiceMetadataConverter
import currency.converter.sources.{CurrencyMappingGenerator, InvoiceMetadataGenerator}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

object App {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setCheckpointInterval(Duration.ofMinutes(1).toMillis)
    checkpointConfig.setMinPauseBetweenCheckpoints(Duration.ofMinutes(1).toMillis)
    checkpointConfig.setTolerableCheckpointFailureNumber(Int.MaxValue)

    val currencyMappings = CurrencyMappingGenerator.create(env)
    val invoiceMetadata = InvoiceMetadataGenerator.create(env, 5)

    val currencyConversions = currencyMappings
      .flatMap((currencyMapping: List[CurrencyConversion], out: Collector[CurrencyConversion]) => currencyMapping.foreach(out.collect))
      .returns(TypeInformation.of(classOf[CurrencyConversion]))
      .name("currency-conversions")
      .broadcast(InvoiceMetadataConverter.CURRENCY_CONVERSION_STATE_DESCRIPTOR)

    val invoiceMetadataInUSD = invoiceMetadata
      .keyBy((i: InvoiceMetadata) => i.id)
      .connect(currencyConversions)
      .process(new InvoiceMetadataConverter())
      .name("invoices-in-USD")

    invoiceMetadataInUSD.print("converted-invoices")

    env.execute("Invoice Currency Converter")
  }
}
