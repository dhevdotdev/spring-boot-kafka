package com.example.orderservice.repository;

import java.util.List;
import java.util.UUID;

import com.example.orderservice.domain.OutboxEvent;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

	/**
	 * The poller's query: the oldest not-yet-published events, in insertion order,
	 * capped to a batch size. Ordering by {@code createdAt} keeps per-order events
	 * in the order they were produced.
	 */
	List<OutboxEvent> findByPublishedAtIsNullOrderByCreatedAtAsc(Limit limit);
}
