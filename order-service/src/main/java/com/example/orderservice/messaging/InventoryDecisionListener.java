package com.example.orderservice.messaging;

import com.example.orderservice.event.InventoryDecisionEvent;
import com.example.orderservice.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes the inventory service's decision and completes the order saga.
 *
 * <p>The payload arrives as a JSON string (see {@code OutboxPoller} on the other
 * side); we parse it explicitly with Jackson. Doing the business work in
 * {@link OrderService} keeps the transaction boundary in the service layer.
 */
@Component
public class InventoryDecisionListener {

	private static final Logger log = LoggerFactory.getLogger(InventoryDecisionListener.class);

	private final OrderService orderService;
	private final ObjectMapper objectMapper;

	public InventoryDecisionListener(OrderService orderService, ObjectMapper objectMapper) {
		this.orderService = orderService;
		this.objectMapper = objectMapper;
	}

	@KafkaListener(topics = Topics.INVENTORY_DECISIONS, groupId = "order-service")
	public void onDecision(String payload) {
		InventoryDecisionEvent event = objectMapper.readValue(payload, InventoryDecisionEvent.class);
		log.info("Received inventory decision {} for order {}", event.decision(), event.orderId());
		orderService.applyInventoryDecision(event);
	}
}
