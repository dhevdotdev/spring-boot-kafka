package com.example.inventoryservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * A stock item, keyed by its product code. Lives only in inventory-service's
 * database — order-service knows nothing about stock levels.
 *
 * <p>The {@code @Version} column gives optimistic locking so two concurrent
 * reservations of the same product can't silently overwrite each other.
 */
@Entity
@Table(name = "product")
public class Product {

	@Id
	private String code;

	@Column(nullable = false)
	private int availableQuantity;

	@Version
	private long version;

	protected Product() {
		// for JPA
	}

	public Product(String code, int availableQuantity) {
		this.code = code;
		this.availableQuantity = availableQuantity;
	}

	public boolean canReserve(int quantity) {
		return availableQuantity >= quantity;
	}

	public void reserve(int quantity) {
		if (!canReserve(quantity)) {
			throw new IllegalStateException("Not enough stock for " + code);
		}
		this.availableQuantity -= quantity;
	}

	public String getCode() {
		return code;
	}

	public int getAvailableQuantity() {
		return availableQuantity;
	}
}
