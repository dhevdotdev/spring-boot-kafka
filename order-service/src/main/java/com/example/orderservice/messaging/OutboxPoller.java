package com.example.orderservice.messaging;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.example.orderservice.domain.OutboxEvent;
import com.example.orderservice.repository.OutboxRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The "relay" half of the transactional outbox. On a fixed schedule it picks up
 * outbox rows that haven't been published yet, sends them to Kafka, and marks
 * them published — all inside one transaction.
 *
 * <p>Delivery is <em>at-least-once</em>: if the app dies after the Kafka send but
 * before the DB commit, the row is still marked unpublished and gets re-sent on
 * the next poll. That's fine — the consumer is idempotent, so a duplicate has no
 * effect. (In production you'd often replace this poller with Debezium CDC
 * reading the same table from the database's write-ahead log.)
 */
@Component
public class OutboxPoller {

	private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
	private static final int BATCH_SIZE = 100;

	private final OutboxRepository outbox;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public OutboxPoller(OutboxRepository outbox, KafkaTemplate<String, String> kafkaTemplate) {
		this.outbox = outbox;
		this.kafkaTemplate = kafkaTemplate;
	}

	@Scheduled(fixedDelayString = "${app.outbox.poll-delay-ms:1000}")
	@Transactional
	public void publishPending() {
		List<OutboxEvent> batch = outbox.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(BATCH_SIZE));
		if (batch.isEmpty()) {
			return;
		}
		log.debug("Outbox: publishing {} pending event(s)", batch.size());
		for (OutboxEvent event : batch) {
			send(event);
			event.markPublished();
		}
	}

	private void send(OutboxEvent event) {
		ProducerRecord<String, String> record =
				new ProducerRecord<>(event.getTopic(), event.getMessageKey(), event.getPayload());
		record.headers().add("event-id", event.getId().toString().getBytes(StandardCharsets.UTF_8));
		record.headers().add("event-type", event.getEventType().getBytes(StandardCharsets.UTF_8));
		try {
			// Block until the broker acks (acks=all). If this throws, the surrounding
			// transaction rolls back and the row stays unpublished for the next poll.
			kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while publishing outbox event " + event.getId(), ex);
		}
		catch (Exception ex) {
			throw new IllegalStateException("Failed to publish outbox event " + event.getId(), ex);
		}
	}
}
