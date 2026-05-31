package com.example.orderservice.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * An order, persisted in order-service's own database.
 *
 * <p>Named {@code OrderEntity} rather than {@code Order} because {@code ORDER}
 * is a reserved SQL word; the table is explicitly named {@code orders}.
 *
 * <p>No Lombok (a Spring Boot 4 house rule) — plain fields with getters/setters.
 * A no-args constructor is required by JPA.
 */
@Entity
@Table(name = "orders")
public class OrderEntity {

	@Id
	private UUID id;

	@Column(nullable = false)
	private String productCode;

	@Column(nullable = false)
	private int quantity;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private OrderStatus status;

	private String statusReason;

	@Column(nullable = false, updatable = false)
	private Instant createdAt;

	protected OrderEntity() {
		// for JPA
	}

	public OrderEntity(UUID id, String productCode, int quantity) {
		this.id = id;
		this.productCode = productCode;
		this.quantity = quantity;
		this.status = OrderStatus.PENDING;
		this.createdAt = Instant.now();
	}

	public void markConfirmed() {
		this.status = OrderStatus.CONFIRMED;
		this.statusReason = null;
	}

	public void markRejected(String reason) {
		this.status = OrderStatus.REJECTED;
		this.statusReason = reason;
	}

	public boolean isPending() {
		return this.status == OrderStatus.PENDING;
	}

	public UUID getId() {
		return id;
	}

	public String getProductCode() {
		return productCode;
	}

	public int getQuantity() {
		return quantity;
	}

	public OrderStatus getStatus() {
		return status;
	}

	public String getStatusReason() {
		return statusReason;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}
}
