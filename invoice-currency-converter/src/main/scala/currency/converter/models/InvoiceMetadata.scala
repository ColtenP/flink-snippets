package currency.converter.models

import java.util.UUID

case class InvoiceMetadata(
    organizationId: UUID,
    id: UUID,
    currency: String,
    amount: Double,
    createdAt: Long,
    settledAt: Long
)
