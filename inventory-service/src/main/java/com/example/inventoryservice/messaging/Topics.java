package com.example.inventoryservice.messaging;

/** Topic names used by inventory-service. */
public final class Topics {

	/** Inbound: order-service → inventory-service. */
	public static final String ORDERS = "orders";

	/** Outbound: inventory-service → order-service. */
	public static final String INVENTORY_DECISIONS = "inventory-decisions";

	private Topics() {
	}
}
