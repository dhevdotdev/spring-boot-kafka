package com.example.inventoryservice.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * The idempotency ledger. Before doing any work for an incoming event we check
 * whether its id is already here; after doing the work we record it — both inside
 * the <em>same</em> transaction as the stock change. A redelivered duplicate then
 * finds its id already present and is skipped, so the side effect happens once.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

	@Id
	private UUID eventId;

	@Column(nullable = false)
	private Instant processedAt;

	protected ProcessedEvent() {
		// for JPA
	}

	public ProcessedEvent(UUID eventId) {
		this.eventId = eventId;
		this.processedAt = Instant.now();
	}

	public UUID getEventId() {
		return eventId;
	}

	public Instant getProcessedAt() {
		return processedAt;
	}
}
