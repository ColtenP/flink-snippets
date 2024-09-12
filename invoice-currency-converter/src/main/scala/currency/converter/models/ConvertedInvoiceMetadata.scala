package currency.converter.models

import java.util.UUID

case class ConvertedInvoiceMetadata(
    organizationId: UUID,
    id: UUID,
    sourceCurrency: String,
    sourceAmount: Double,
    convertedCurrency: String,
    convertedAmount: Double,
    createdAt: Long,
    settledAt: Long
)
