package com.example.inventoryservice.service;

import java.util.Optional;

import com.example.inventoryservice.domain.ProcessedEvent;
import com.example.inventoryservice.domain.Product;
import com.example.inventoryservice.event.InventoryDecisionEvent;
import com.example.inventoryservice.event.OrderPlacedEvent;
import com.example.inventoryservice.repository.ProcessedEventRepository;
import com.example.inventoryservice.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reserves stock for an order, idempotently.
 *
 * <p>The whole method runs in one transaction. The idempotency check and the
 * idempotency write bracket the business logic, so the stock decrement and the
 * "I've seen this event" record commit together or not at all.
 */
@Service
public class InventoryService {

	private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

	/** Magic product code that simulates a transient infrastructure failure. */
	private static final String POISON_PRODUCT = "BOOM";

	private final ProductRepository products;
	private final ProcessedEventRepository processedEvents;

	public InventoryService(ProductRepository products, ProcessedEventRepository processedEvents) {
		this.products = products;
		this.processedEvents = processedEvents;
	}

	/**
	 * @return the decision to publish, or empty if this event was already processed
	 *         (a duplicate delivery) and should be silently ignored.
	 */
	@Transactional
	public Optional<InventoryDecisionEvent> reserve(OrderPlacedEvent event) {
		if (processedEvents.existsById(event.eventId())) {
			log.info("Duplicate event {} for order {} — already processed, skipping",
					event.eventId(), event.orderId());
			return Optional.empty();
		}

		// Simulated transient failure: thrown BEFORE we record the event as
		// processed, so every retry re-runs this and the message eventually
		// lands in the dead-letter topic.
		if (POISON_PRODUCT.equalsIgnoreCase(event.productCode())) {
			throw new InventoryProcessingException(
					"Simulated downstream failure processing order " + event.orderId());
		}

		InventoryDecisionEvent decision = decide(event);
		processedEvents.save(new ProcessedEvent(event.eventId()));
		return Optional.of(decision);
	}

	private InventoryDecisionEvent decide(OrderPlacedEvent event) {
		Optional<Product> maybe = products.findById(event.productCode());
		if (maybe.isEmpty()) {
			log.info("Order {} rejected: unknown product {}", event.orderId(), event.productCode());
			return InventoryDecisionEvent.rejected(event, "Unknown product " + event.productCode());
		}
		Product product = maybe.get();
		if (!product.canReserve(event.quantity())) {
			log.info("Order {} rejected: only {} of {} left, wanted {}",
					event.orderId(), product.getAvailableQuantity(), product.getCode(), event.quantity());
			return InventoryDecisionEvent.rejected(event,
					"Insufficient stock for " + product.getCode());
		}
		product.reserve(event.quantity());
		products.save(product);
		log.info("Order {} reserved: {} x{} ({} left)",
				event.orderId(), product.getCode(), event.quantity(), product.getAvailableQuantity());
		return InventoryDecisionEvent.reserved(event);
	}
}
