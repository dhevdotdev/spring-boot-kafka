package com.example.orderservice.messaging;

/** Topic names shared by the producer and consumer sides of order-service. */
public final class Topics {

	/** Outbound: order-service → inventory-service. */
	public static final String ORDERS = "orders";

	/** Inbound: inventory-service → order-service (the saga reply). */
	public static final String INVENTORY_DECISIONS = "inventory-decisions";

	private Topics() {
	}
}
