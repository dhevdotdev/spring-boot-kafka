package com.example.orderservice.event;

import java.time.Instant;
import java.util.UUID;

/**
 * The event order-service publishes to the {@code orders} topic.
 *
 * <p>This is the <em>contract</em> between the two services. In this demo each
 * service keeps its own copy of the record (no shared jar) — see the README for
 * why that's a deliberate choice and what you'd use in production (a schema
 * registry with Avro/Protobuf). It's a plain Java record serialized to JSON.
 *
 * @param eventId     unique id for this event; consumers deduplicate on it
 * @param orderId     the order this event is about
 * @param productCode what was ordered
 * @param quantity    how many
 * @param occurredAt  when the order was placed
 */
public record OrderPlacedEvent(
		UUID eventId,
		UUID orderId,
		String productCode,
		int quantity,
		Instant occurredAt) {
}
