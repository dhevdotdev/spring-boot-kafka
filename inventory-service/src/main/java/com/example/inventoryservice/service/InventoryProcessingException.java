package com.example.inventoryservice.service;

/**
 * A <em>transient</em> failure while processing an event — the kind that's worth
 * retrying (a flaky downstream call, a brief outage). We use it to simulate such
 * a failure so you can watch the retry topics and the dead-letter topic in action.
 */
public class InventoryProcessingException extends RuntimeException {

	public InventoryProcessingException(String message) {
		super(message);
	}
}
