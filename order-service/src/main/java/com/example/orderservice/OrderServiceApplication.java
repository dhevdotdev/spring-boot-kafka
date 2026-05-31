package com.example.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * order-service: the "command" side of the demo.
 *
 * <p>It accepts orders over REST, stores them in its own database, and reliably
 * hands "OrderPlaced" events to Kafka using the transactional outbox pattern.
 * It also listens for the inventory service's decision to complete the saga.
 *
 * <p>{@code @EnableScheduling} powers the {@code OutboxPoller}.
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderServiceApplication.class, args);
	}

}
