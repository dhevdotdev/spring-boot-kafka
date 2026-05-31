package com.example.orderservice.event;

import java.time.Instant;
import java.util.UUID;

/**
 * The reply event order-service consumes from the {@code inventory-decisions}
 * topic. It closes the saga: the order moves from PENDING to CONFIRMED/REJECTED.
 *
 * @param decision "RESERVED" or "REJECTED"
 */
public record InventoryDecisionEvent(
		UUID eventId,
		UUID orderId,
		String productCode,
		int quantity,
		String decision,
		String reason,
		Instant occurredAt) {

	public boolean isReserved() {
		return "RESERVED".equalsIgnoreCase(decision);
	}
}
