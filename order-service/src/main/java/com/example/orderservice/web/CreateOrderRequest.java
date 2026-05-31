package com.example.orderservice.web;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/** Request body for {@code POST /api/orders}. */
public record CreateOrderRequest(
		@NotBlank String productCode,
		@Min(1) int quantity) {
}
