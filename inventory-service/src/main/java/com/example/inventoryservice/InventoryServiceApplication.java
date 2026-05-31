package com.example.inventoryservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * inventory-service: the "reaction" side of the demo.
 *
 * <p>It consumes OrderPlaced events, reserves stock <em>idempotently</em>
 * (at-least-once delivery + dedupe = effectively-once processing), retries on
 * transient failures with a dead-letter topic as the backstop, and publishes its
 * decision back so order-service can finish the saga.
 */
@SpringBootApplication
public class InventoryServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(InventoryServiceApplication.class, args);
	}

}
