package com.example.orderservice.web;

import java.time.Instant;
import java.util.UUID;

import com.example.orderservice.domain.OrderEntity;
import com.example.orderservice.domain.OrderStatus;

/** Response view of an order. */
public record OrderResponse(
		UUID id,
		String productCode,
		int quantity,
		OrderStatus status,
		String statusReason,
		Instant createdAt) {

	public static OrderResponse from(OrderEntity order) {
		return new OrderResponse(
				order.getId(),
				order.getProductCode(),
				order.getQuantity(),
				order.getStatus(),
				order.getStatusReason(),
				order.getCreatedAt());
	}
}
