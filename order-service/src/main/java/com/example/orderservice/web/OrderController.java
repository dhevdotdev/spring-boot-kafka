package com.example.orderservice.web;

import java.net.URI;
import java.util.UUID;

import com.example.orderservice.domain.OrderEntity;
import com.example.orderservice.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST entry point. {@code POST} places an order (returns immediately with status
 * PENDING — the confirmation happens asynchronously), {@code GET} reads it back so
 * you can watch it flip to CONFIRMED or REJECTED.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

	private final OrderService orderService;

	public OrderController(OrderService orderService) {
		this.orderService = orderService;
	}

	@PostMapping
	public ResponseEntity<OrderResponse> place(@Valid @RequestBody CreateOrderRequest request) {
		OrderEntity order = orderService.placeOrder(request.productCode(), request.quantity());
		return ResponseEntity
				.created(URI.create("/api/orders/" + order.getId()))
				.body(OrderResponse.from(order));
	}

	@GetMapping("/{id}")
	public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
		return orderService.findOrder(id)
				.map(OrderResponse::from)
				.map(ResponseEntity::ok)
				.orElseGet(() -> ResponseEntity.notFound().build());
	}
}
