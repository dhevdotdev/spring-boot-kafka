package com.example.orderservice.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * One row in the transactional outbox.
 *
 * <p>The whole point of this table: it lives in the <em>same database</em> as
 * {@link OrderEntity}, so an order and its "to be published" event are written
 * in a single ACID transaction. A separate poller later ships the row to Kafka.
 * That removes the "dual write" race where the DB commit succeeds but the Kafka
 * publish fails (or vice-versa).
 *
 * <p>{@code id} doubles as the Kafka event id, so consumers can deduplicate on it.
 */
@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

	@Id
	private UUID id;

	/** Logical Kafka topic this event should be published to. */
	@Column(nullable = false)
	private String topic;

	/** Kafka message key — we use the aggregate (order) id to preserve per-order ordering. */
	@Column(nullable = false)
	private String messageKey;

	/** Short event type name, also written as a Kafka header. */
	@Column(nullable = false)
	private String eventType;

	/** The serialized JSON payload, stored as text. */
	@Column(nullable = false, columnDefinition = "text")
	private String payload;

	@Column(nullable = false, updatable = false)
	private Instant createdAt;

	/** Null until the poller has successfully published this row. */
	private Instant publishedAt;

	protected OutboxEvent() {
		// for JPA
	}

	public OutboxEvent(UUID id, String topic, String messageKey, String eventType, String payload) {
		this.id = id;
		this.topic = topic;
		this.messageKey = messageKey;
		this.eventType = eventType;
		this.payload = payload;
		this.createdAt = Instant.now();
	}

	public void markPublished() {
		this.publishedAt = Instant.now();
	}

	public UUID getId() {
		return id;
	}

	public String getTopic() {
		return topic;
	}

	public String getMessageKey() {
		return messageKey;
	}

	public String getEventType() {
		return eventType;
	}

	public String getPayload() {
		return payload;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public Instant getPublishedAt() {
		return publishedAt;
	}
}
