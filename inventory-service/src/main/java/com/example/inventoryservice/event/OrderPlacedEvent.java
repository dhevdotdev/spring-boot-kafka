package com.example.inventoryservice.event;

import java.time.Instant;
import java.util.UUID;

/**
 * inventory-service's own copy of the {@code orders} contract. It only declares
 * the fields it cares about; Jackson ignores any extra fields the producer adds
 * (forward compatibility). Keeping a private copy means the two services can be
 * built and deployed independently — see the README's note on contract sharing.
 */
public record OrderPlacedEvent(
		UUID eventId,
		UUID orderId,
		String productCode,
		int quantity,
		Instant occurredAt) {
}
