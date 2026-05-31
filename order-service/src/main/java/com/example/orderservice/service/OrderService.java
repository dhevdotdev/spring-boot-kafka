package com.example.orderservice.service;

import java.util.Optional;
import java.util.UUID;

import com.example.orderservice.domain.OrderEntity;
import com.example.orderservice.domain.OutboxEvent;
import com.example.orderservice.event.InventoryDecisionEvent;
import com.example.orderservice.event.OrderPlacedEvent;
import com.example.orderservice.messaging.Topics;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Business logic for orders. Two responsibilities:
 *
 * <ol>
 *   <li>{@link #placeOrder} — create an order and stage its event in the outbox,
 *       atomically, in one DB transaction (the transactional outbox pattern).</li>
 *   <li>{@link #applyInventoryDecision} — react to the inventory service's reply
 *       and move the order out of PENDING (the end of the saga).</li>
 * </ol>
 *
 * Note there is <em>no</em> {@code kafkaTemplate.send(...)} here. The service only
 * touches its own database; publishing is the poller's job. That separation is
 * what makes the write reliable.
 */
@Service
public class OrderService {

	private static final Logger log = LoggerFactory.getLogger(OrderService.class);

	private final OrderRepository orders;
	private final OutboxRepository outbox;
	private final ObjectMapper objectMapper;

	public OrderService(OrderRepository orders, OutboxRepository outbox, ObjectMapper objectMapper) {
		this.orders = orders;
		this.outbox = outbox;
		this.objectMapper = objectMapper;
	}

	@Transactional
	public OrderEntity placeOrder(String productCode, int quantity) {
		OrderEntity order = new OrderEntity(UUID.randomUUID(), productCode, quantity);
		orders.save(order);

		OrderPlacedEvent event = new OrderPlacedEvent(
				UUID.randomUUID(), order.getId(), productCode, quantity, order.getCreatedAt());

		// id of the outbox row == eventId, so the consumer can dedupe on it.
		OutboxEvent row = new OutboxEvent(
				event.eventId(),
				Topics.ORDERS,
				order.getId().toString(),
				"OrderPlaced",
				objectMapper.writeValueAsString(event));
		outbox.save(row);

		log.info("Placed order {} for {} x{} (event {} staged in outbox)",
				order.getId(), productCode, quantity, event.eventId());
		return order;
	}

	/**
	 * Apply the inventory decision. Idempotent: a duplicate delivery (Kafka is
	 * at-least-once) finds the order is no longer PENDING and does nothing.
	 */
	@Transactional
	public void applyInventoryDecision(InventoryDecisionEvent decision) {
		Optional<OrderEntity> maybe = orders.findById(decision.orderId());
		if (maybe.isEmpty()) {
			log.warn("Decision for unknown order {} — ignoring", decision.orderId());
			return;
		}
		OrderEntity order = maybe.get();
		if (!order.isPending()) {
			log.info("Order {} already in state {} — ignoring duplicate decision",
					order.getId(), order.getStatus());
			return;
		}
		if (decision.isReserved()) {
			order.markConfirmed();
			log.info("Order {} CONFIRMED", order.getId());
		} else {
			order.markRejected(decision.reason());
			log.info("Order {} REJECTED: {}", order.getId(), decision.reason());
		}
		orders.save(order);
	}

	@Transactional(readOnly = true)
	public Optional<OrderEntity> findOrder(UUID id) {
		return orders.findById(id);
	}
}
