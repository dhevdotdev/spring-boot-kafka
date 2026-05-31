package com.example.inventoryservice.messaging;

import com.example.inventoryservice.event.OrderPlacedEvent;
import com.example.inventoryservice.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes OrderPlaced events.
 *
 * <p>{@code @RetryableTopic} gives <em>non-blocking</em> retries: if the listener
 * throws, the record is forwarded to an auto-created {@code orders-retry} topic
 * with an increasing delay, leaving the main partition free to keep processing
 * other orders. After the attempts are exhausted the record is parked in the
 * {@code orders-dlt} dead-letter topic, handled by {@link #onDlt}.
 *
 * <p>Try ordering product {@code BOOM} to watch this path; the service throws a
 * transient error every time, so it retries and then lands in the DLT.
 */
@Component
public class OrderEventListener {

	private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

	private final InventoryService inventoryService;
	private final InventoryDecisionPublisher publisher;
	private final ObjectMapper objectMapper;

	public OrderEventListener(InventoryService inventoryService, InventoryDecisionPublisher publisher,
			ObjectMapper objectMapper) {
		this.inventoryService = inventoryService;
		this.publisher = publisher;
		this.objectMapper = objectMapper;
	}

	@RetryableTopic(
			attempts = "4",
			backOff = @BackOff(delay = 1000, multiplier = 2.0),
			autoCreateTopics = "true")
	@KafkaListener(topics = Topics.ORDERS, groupId = "inventory-service")
	public void onOrder(String payload) {
		OrderPlacedEvent event = objectMapper.readValue(payload, OrderPlacedEvent.class);
		log.info("Received order {} for {} x{}", event.orderId(), event.productCode(), event.quantity());
		inventoryService.reserve(event).ifPresent(publisher::publish);
	}

	@DltHandler
	public void onDlt(String payload,
			@Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
			@Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String errorMessage) {
		log.error("DEAD LETTER: order event exhausted retries. origin-topic={}, error='{}', payload={}",
				originalTopic, errorMessage, payload);
		// In production you'd alert here and/or store the record for manual replay.
	}
}
