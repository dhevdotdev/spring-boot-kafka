package com.example.orderservice.domain;

/**
 * Lifecycle of an order in this demo.
 *
 * <pre>
 *   PENDING ──(inventory reserved)──▶ CONFIRMED
 *           └─(inventory rejected)──▶ REJECTED
 * </pre>
 *
 * The transition out of PENDING is driven asynchronously by an event from the
 * inventory service, which is what makes this a (tiny) choreographed saga.
 */
public enum OrderStatus {
	PENDING,
	CONFIRMED,
	REJECTED
}
