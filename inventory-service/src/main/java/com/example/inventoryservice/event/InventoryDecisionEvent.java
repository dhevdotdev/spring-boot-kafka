package com.example.inventoryservice.event;

import java.time.Instant;
import java.util.UUID;

/** The decision inventory-service publishes back to order-service. */
public record InventoryDecisionEvent(
		UUID eventId,
		UUID orderId,
		String productCode,
		int quantity,
		String decision,
		String reason,
		Instant occurredAt) {

	public static InventoryDecisionEvent reserved(OrderPlacedEvent order) {
		return new InventoryDecisionEvent(UUID.randomUUID(), order.orderId(), order.productCode(),
				order.quantity(), "RESERVED", null, Instant.now());
	}

	public static InventoryDecisionEvent rejected(OrderPlacedEvent order, String reason) {
		return new InventoryDecisionEvent(UUID.randomUUID(), order.orderId(), order.productCode(),
				order.quantity(), "REJECTED", reason, Instant.now());
	}
}
