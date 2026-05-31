package com.example.inventoryservice.messaging;

import java.nio.charset.StandardCharsets;

import com.example.inventoryservice.event.InventoryDecisionEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/** Publishes inventory decisions back to order-service. */
@Component
public class InventoryDecisionPublisher {

	private static final Logger log = LoggerFactory.getLogger(InventoryDecisionPublisher.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;

	public InventoryDecisionPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	public void publish(InventoryDecisionEvent decision) {
		ProducerRecord<String, String> record = new ProducerRecord<>(
				Topics.INVENTORY_DECISIONS,
				decision.orderId().toString(),
				objectMapper.writeValueAsString(decision));
		record.headers().add("event-id", decision.eventId().toString().getBytes(StandardCharsets.UTF_8));
		record.headers().add("event-type", "InventoryDecision".getBytes(StandardCharsets.UTF_8));
		kafkaTemplate.send(record).whenComplete((result, ex) -> {
			if (ex != null) {
				log.error("Failed to publish decision for order {}", decision.orderId(), ex);
			}
			else {
				log.debug("Published {} decision for order {}", decision.decision(), decision.orderId());
			}
		});
	}
}
