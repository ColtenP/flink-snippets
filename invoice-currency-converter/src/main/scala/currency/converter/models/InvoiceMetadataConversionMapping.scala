package currency.converter.models

import java.util.UUID

case class InvoiceMetadataConversionMapping(
    organizationId: UUID,
    id: UUID,
    currency: String,
    amount: Double,
    createdAt: Long,
    settledAt: Long,
    conversions: Map[String, Double]
)
