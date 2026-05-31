package com.example.inventoryservice.config;

import com.example.inventoryservice.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declare the topics this service interacts with. Creating them from both
 * services is harmless — {@code KafkaAdmin} creation is idempotent — and means
 * either service can be started first. The {@code orders-retry}/{@code orders-dlt}
 * topics are created automatically by {@code @RetryableTopic}.
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
