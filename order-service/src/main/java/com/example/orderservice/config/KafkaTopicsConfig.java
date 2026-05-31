package com.example.orderservice.config;

import com.example.orderservice.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declaratively create the topics this service owns. Spring's {@code KafkaAdmin}
 * picks up every {@link NewTopic} bean and creates it at startup if missing.
 *
 * <p>For a local single-broker demo we use 3 partitions and a replication factor
 * of 1. In production the replication factor would be at least 3.
 */
@Configuration
public class KafkaTopicsConfig {

	@Bean
	NewTopic ordersTopic() {
		return TopicBuilder.name(Topics.ORDERS).partitions(3).replicas(1).build();
	}

	@Bean
	NewTopic inventoryDecisionsTopic() {
		return TopicBuilder.name(Topics.INVENTORY_DECISIONS).partitions(3).replicas(1).build();
	}
}
